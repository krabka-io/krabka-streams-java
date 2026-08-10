package io.krabka.streams.columnar;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Runs fetch, process, asynchronously produce, and commit cycles.
 *
 * <p>The runner is the bridge between plain Kafka clients and a columnar topology.
 * Each cycle polls the consumer, evaluates the topology per logical partition,
 * produces every output record concurrently, and commits offsets only after all
 * acknowledgements arrive — so a crash mid-cycle reprocesses rather than loses
 * records. On failure the affected partition's operator state is rolled back to its
 * pre-batch snapshot before the error policy is applied.
 *
 * <p>Three styles are available: the static {@code runPartitionOnce} methods for a
 * manually assigned partition, the static {@code runGroupOnce} methods for a
 * subscribed consumer group, and {@link #group group}, which returns a
 * {@link GroupRunner} that also manages rebalances and persistent operator state.
 * All methods drive the callers' consumer and producer; the runner creates no clients
 * and owns no threads, so the caller controls the loop, threading, and shutdown.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try (var runner = ColumnarRunner.group(topology, consumer, producer,
 *         ColumnarErrorPolicy.deadLetter("transactions-dead-letter"),
 *         new FileColumnarStateStore(stateDirectory),
 *         new ColumnarMetrics())) {
 *     while (running) {
 *         runner.runOnce(Duration.ofSeconds(1));
 *     }
 * }
 * }</pre>
 */
public final class ColumnarRunner {
    private ColumnarRunner() {
    }

    /**
     * Builds the topology, runs one cycle for one partition, and discards the state.
     *
     * <p>Because the built topology is thrown away, stateful operators start fresh
     * every call; use the {@link BuiltColumnarTopology} overload to accumulate state
     * across cycles.
     *
     * @param topology the topology to build and run once
     * @param consumer the consumer to assign and poll; its subscription is replaced
     * @param producer the producer output records are sent with
     * @param topic the source topic to read
     * @param partition the partition to read
     * @param offset the offset to seek to before polling
     * @param pollTimeout how long one poll may block
     * @return the next offset to process from; {@code offset} itself when the poll
     *     was empty
     */
    public static long runPartitionOnce(
            ColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer,
            String topic,
            int partition,
            long offset,
            Duration pollTimeout) {
        try (var built = Objects.requireNonNull(topology, "topology").build()) {
            return runPartitionOnce(
                    built, consumer, producer, topic, partition, offset, pollTimeout);
        }
    }

    /**
     * Runs one fetch, process, produce, and commit cycle for one manually assigned
     * partition.
     *
     * <p>The consumer is assigned to the partition and sought to the offset. When the
     * poll returns records, the batch is processed, every output is produced and
     * acknowledged, and the next offset is committed synchronously. If any step
     * fails, the partition's operator state is rolled back to its pre-batch snapshot
     * and the error is rethrown, so the caller can retry from the same offset.
     *
     * @param topology the built topology whose partition state carries across calls
     * @param consumer the consumer to assign and poll; its subscription is replaced
     * @param producer the producer output records are sent with
     * @param topic the source topic to read
     * @param partition the partition to read
     * @param offset the offset to seek to before polling
     * @param pollTimeout how long one poll may block
     * @return the next offset to process from; {@code offset} itself when the poll
     *     was empty
     */
    public static long runPartitionOnce(
            BuiltColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer,
            String topic,
            int partition,
            long offset,
            Duration pollTimeout) {
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(producer, "producer");
        var topicPartition = new TopicPartition(topic, partition);
        consumer.assign(List.of(topicPartition));
        consumer.seek(topicPartition, offset);
        var polled = consumer.poll(pollTimeout).records(topicPartition);
        if (polled.isEmpty()) {
            return offset;
        }

        var records = polled.stream().map(ColumnarRunner::consumed).toList();
        long nextOffset = polled.stream()
                .mapToLong(record -> Math.addExact(record.offset(), 1))
                .max()
                .orElse(offset);
        var prior = new PriorState(topology.hasPartition(partition), topology.snapshotPartition(partition));
        try {
            sendAsync(topology.runPartitionBatches(partition, Map.of(topic, records)), producer).join();
            consumer.commitSync(Map.of(topicPartition, new OffsetAndMetadata(nextOffset)));
            return nextOffset;
        } catch (RuntimeException error) {
            restore(topology, partition, prior);
            throw error;
        }
    }

    /**
     * Polls and processes every partition assigned by the consumer group, then
     * commits their offsets.
     *
     * <p>Fail-fast form: any processing failure rolls the affected partitions back
     * and rethrows. Equivalent to the six-argument overload with
     * {@link ColumnarErrorPolicy#fail()} and throwaway metrics.
     *
     * @param topology the built topology whose partition state carries across calls
     * @param consumer the subscribed consumer to poll
     * @param producer the producer output records are sent with
     * @param pollTimeout how long one poll may block
     * @return topic partition to next offset for everything committed this cycle;
     *     empty when the poll returned nothing
     */
    public static Map<TopicPartition, Long> runGroupOnce(
            BuiltColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer,
            Duration pollTimeout) {
        return runGroupOnce(
                topology,
                consumer,
                producer,
                pollTimeout,
                ColumnarErrorPolicy.fail(),
                new ColumnarMetrics());
    }

    /**
     * Polls and processes every assigned partition with explicit failure handling
     * and metrics.
     *
     * <p>Each logical partition is processed independently: a partition that fails is
     * rolled back and handled per the error policy, while the others' output is still
     * produced and committed. If producing or committing fails, every partition of
     * the cycle is rolled back and the error is rethrown.
     *
     * @param topology the built topology whose partition state carries across calls
     * @param consumer the subscribed consumer to poll
     * @param producer the producer output records are sent with
     * @param pollTimeout how long one poll may block
     * @param errorPolicy what to do with a partition whose processing fails
     * @param metrics the counters one observation per partition batch is recorded to
     * @return topic partition to next offset for everything committed this cycle;
     *     empty when the poll returned nothing
     */
    public static Map<TopicPartition, Long> runGroupOnce(
            BuiltColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer,
            Duration pollTimeout,
            ColumnarErrorPolicy errorPolicy,
            ColumnarMetrics metrics) {
        var poll = processPoll(topology, consumer, pollTimeout, errorPolicy, metrics);
        try {
            sendAsync(poll.outputs(), producer).join();
            if (!poll.offsets().isEmpty()) {
                consumer.commitSync(poll.offsets());
            }
            return nextOffsets(poll.offsets());
        } catch (RuntimeException error) {
            poll.rollback();
            throw error;
        }
    }

    /**
     * Atomically produces a group poll and commits its consumed offsets in the
     * producer transaction.
     *
     * <p>Fail-fast form of the transactional cycle; see the six-argument overload.
     * The producer must be transactional and already initialized with
     * {@code initTransactions()}.
     *
     * @param topology the built topology whose partition state carries across calls
     * @param consumer the subscribed consumer to poll
     * @param producer the transactional producer output records are sent with
     * @param pollTimeout how long one poll may block
     * @return topic partition to next offset for everything committed this cycle;
     *     empty when the poll returned nothing
     */
    public static Map<TopicPartition, Long> runGroupOnceTransactional(
            BuiltColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer,
            Duration pollTimeout) {
        return runGroupOnceTransactional(
                topology,
                consumer,
                producer,
                pollTimeout,
                ColumnarErrorPolicy.fail(),
                new ColumnarMetrics());
    }

    /**
     * Runs one exactly-once cycle: output records and consumed offsets commit in one
     * producer transaction.
     *
     * <p>The whole poll — output records and offset advances — becomes visible
     * atomically or not at all. On any failure the transaction is aborted, every
     * partition of the cycle is rolled back, and the error is rethrown. The producer
     * must be transactional and already initialized with {@code initTransactions()};
     * downstream consumers need {@code isolation.level=read_committed} to observe
     * exactly-once behavior.
     *
     * @param topology the built topology whose partition state carries across calls
     * @param consumer the subscribed consumer to poll
     * @param producer the transactional producer output records are sent with
     * @param pollTimeout how long one poll may block
     * @param errorPolicy what to do with a partition whose processing fails
     * @param metrics the counters one observation per partition batch is recorded to
     * @return topic partition to next offset for everything committed this cycle;
     *     empty when the poll returned nothing
     */
    public static Map<TopicPartition, Long> runGroupOnceTransactional(
            BuiltColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer,
            Duration pollTimeout,
            ColumnarErrorPolicy errorPolicy,
            ColumnarMetrics metrics) {
        producer.beginTransaction();
        ProcessedPoll poll = null;
        try {
            poll = processPoll(topology, consumer, pollTimeout, errorPolicy, metrics);
            sendAsync(poll.outputs(), producer).join();
            if (!poll.offsets().isEmpty()) {
                producer.sendOffsetsToTransaction(poll.offsets(), consumer.groupMetadata());
            }
            producer.commitTransaction();
            return nextOffsets(poll.offsets());
        } catch (RuntimeException error) {
            producer.abortTransaction();
            if (poll != null) {
                poll.rollback();
            }
            throw error;
        }
    }

    /**
     * Sends all records concurrently and completes when every broker acknowledgement
     * arrives.
     *
     * <p>Records enter the producer in list order but acknowledgements may arrive in
     * any order. A record whose timestamp is negative is sent without a timestamp, so
     * the producer or broker assigns one. The returned future fails when any send
     * fails; the remaining sends are not canceled.
     *
     * @param outputs the records to send, each bound to its topic
     * @param producer the producer to send with
     * @return a future that completes when every record is acknowledged
     */
    public static CompletableFuture<Void> sendAsync(
            List<ProducedToTopic> outputs, Producer<byte[], byte[]> producer) {
        Objects.requireNonNull(outputs, "outputs");
        Objects.requireNonNull(producer, "producer");
        var sends = new ArrayList<CompletableFuture<Void>>(outputs.size());
        for (var output : outputs) {
            var record = output.record();
            Long timestamp = record.timestamp() < 0 ? null : record.timestamp();
            var headers = record.headers().stream()
                    .map(header -> (org.apache.kafka.common.header.Header)
                            new org.apache.kafka.common.header.internals.RecordHeader(
                                    header.key(), header.value()))
                    .toList();
            var send = new CompletableFuture<Void>();
            try {
                producer.send(
                        new ProducerRecord<byte[], byte[]>(
                                output.topic(), null, timestamp, record.key(), record.value(), headers),
                        (metadata, error) -> {
                            if (error == null) {
                                send.complete(null);
                            } else {
                                send.completeExceptionally(error);
                            }
                        });
            } catch (RuntimeException error) {
                send.completeExceptionally(error);
            }
            sends.add(send);
        }
        return CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new));
    }

    /**
     * Subscribes a group-managed consumer to every source topic in the topology.
     *
     * <p>Use this together with the static {@code runGroupOnce} methods;
     * {@link #group group} subscribes its consumer itself.
     *
     * @param topology the topology whose source topics are subscribed to
     * @param consumer the group-managed consumer to subscribe
     */
    public static void subscribe(ColumnarTopology topology, Consumer<byte[], byte[]> consumer) {
        consumer.subscribe(topology.sourceTopics());
    }

    /**
     * Creates an ephemeral, fail-fast group runner.
     *
     * <p>Equivalent to the six-argument overload with
     * {@link ColumnarErrorPolicy#fail()}, {@link ColumnarStateStore#none()}, and
     * fresh metrics: processing failures are rethrown and operator state does not
     * survive rebalances.
     *
     * @param topology the topology to build and run; the runner owns the built form
     * @param consumer the consumer the runner polls; it is subscribed to the
     *     topology's source topics
     * @param producer the producer output records are sent with
     * @return the runner; close it to save nothing and release processor state
     */
    public static GroupRunner group(
            ColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer) {
        return group(
                topology,
                consumer,
                producer,
                ColumnarErrorPolicy.fail(),
                ColumnarStateStore.none(),
                new ColumnarMetrics());
    }

    /**
     * Creates a group runner with explicit failure, snapshot, and metric handling.
     *
     * <p>The runner builds the topology, subscribes the consumer to its source topics
     * with itself as the rebalance listener, and from then on loads operator state
     * from the state store when a logical partition is assigned and saves it back
     * when the partition is revoked or the runner closes. Lost partitions are
     * released without saving.
     *
     * @param topology the topology to build and run; the runner owns the built form
     * @param consumer the consumer the runner polls; it is subscribed to the
     *     topology's source topics
     * @param producer the producer output records are sent with
     * @param errorPolicy what to do with a partition whose processing fails
     * @param stateStore where operator snapshots are loaded from and saved to
     * @param metrics the counters the runner records observations to
     * @return the runner; close it to save state and release the built topology
     */
    public static GroupRunner group(
            ColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer,
            ColumnarErrorPolicy errorPolicy,
            ColumnarStateStore stateStore,
            ColumnarMetrics metrics) {
        Objects.requireNonNull(topology, "topology");
        var runner = new GroupRunner(
                topology.build(),
                Objects.requireNonNull(consumer, "consumer"),
                Objects.requireNonNull(producer, "producer"),
                Objects.requireNonNull(errorPolicy, "errorPolicy"),
                Objects.requireNonNull(stateStore, "stateStore"),
                Objects.requireNonNull(metrics, "metrics"));
        consumer.subscribe(topology.sourceTopics(), runner);
        return runner;
    }

    private static ProcessedPoll processPoll(
            BuiltColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Duration pollTimeout,
            ColumnarErrorPolicy errorPolicy,
            ColumnarMetrics metrics) {
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(consumer, "consumer");
        var polled = consumer.poll(pollTimeout);
        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        var byPartition = new java.util.TreeMap<Integer, Map<String, List<ConsumedRecord>>>();
        for (var topicPartition : polled.partitions()) {
            var records = polled.records(topicPartition).stream().map(ColumnarRunner::consumed).toList();
            byPartition.computeIfAbsent(topicPartition.partition(), ignored -> new HashMap<>())
                    .put(topicPartition.topic(), records);
            offsets.put(topicPartition, new OffsetAndMetadata(
                    Math.addExact(polled.records(topicPartition).get(records.size() - 1).offset(), 1)));
        }

        var outputs = new ArrayList<ProducedToTopic>();
        var prior = new HashMap<Integer, PriorState>();
        try {
            for (var entry : byPartition.entrySet()) {
                int partition = entry.getKey();
                var input = entry.getValue();
                var before = new PriorState(topology.hasPartition(partition), topology.snapshotPartition(partition));
                prior.put(partition, before);
                int inputCount = input.values().stream().mapToInt(List::size).sum();
                long started = System.nanoTime();
                try {
                    var partitionOutput = topology.runPartitionBatches(partition, input);
                    outputs.addAll(partitionOutput);
                    metrics.recordBatch(inputCount, partitionOutput.size(), System.nanoTime() - started);
                } catch (RuntimeException error) {
                    restore(topology, partition, before);
                    prior.remove(partition);
                    if (errorPolicy.action() == ColumnarErrorPolicy.Action.FAIL || retriable(error)) {
                        throw error;
                    }
                    int deadLetters = 0;
                    if (errorPolicy.action() == ColumnarErrorPolicy.Action.DEAD_LETTER) {
                        for (var topicRecords : input.entrySet()) {
                            for (var record : topicRecords.getValue()) {
                                outputs.add(deadLetter(errorPolicy.deadLetterTopic(), topicRecords.getKey(), record, error));
                                deadLetters++;
                            }
                        }
                    }
                    metrics.recordFailure(inputCount, deadLetters, System.nanoTime() - started);
                }
            }
            return new ProcessedPoll(topology, Map.copyOf(offsets), List.copyOf(outputs), Map.copyOf(prior));
        } catch (RuntimeException error) {
            prior.forEach((partition, state) -> restore(topology, partition, state));
            throw error;
        }
    }

    private static boolean retriable(Throwable error) {
        for (var cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof org.apache.kafka.common.errors.RetriableException) {
                return true;
            }
        }
        return false;
    }

    private static ConsumedRecord consumed(org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record) {
        var headers = new ArrayList<RecordHeader>();
        record.headers().forEach(header -> headers.add(new RecordHeader(header.key(), header.value())));
        return new ConsumedRecord(
                record.key(),
                record.value() == null ? new byte[0] : record.value(),
                record.timestamp(),
                record.partition(),
                record.offset(),
                headers);
    }

    private static ProducedToTopic deadLetter(
            String deadLetterTopic, String sourceTopic, ConsumedRecord record, RuntimeException error) {
        var headers = new ArrayList<>(record.headers());
        headers.add(textHeader("krabka.error.class", error.getClass().getName()));
        headers.add(textHeader("krabka.error.message", String.valueOf(error.getMessage())));
        headers.add(textHeader("krabka.source.topic", sourceTopic));
        headers.add(textHeader("krabka.source.partition", Integer.toString(record.partition())));
        headers.add(textHeader("krabka.source.offset", Long.toString(record.offset())));
        return new ProducedToTopic(
                deadLetterTopic,
                new ProduceRecord(record.key(), record.value(), record.timestamp(), headers));
    }

    private static RecordHeader textHeader(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void restore(BuiltColumnarTopology topology, int partition, PriorState prior) {
        topology.releasePartition(partition);
        if (prior.existed()) {
            topology.restorePartition(partition, prior.snapshot());
        }
    }

    private static Map<TopicPartition, Long> nextOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        return offsets.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey, entry -> entry.getValue().offset()));
    }

    private record PriorState(boolean existed, Map<String, byte[]> snapshot) {
    }

    private record ProcessedPoll(
            BuiltColumnarTopology topology,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            List<ProducedToTopic> outputs,
            Map<Integer, PriorState> prior) {
        private void rollback() {
            prior.forEach((partition, state) -> restore(topology, partition, state));
        }
    }

    /**
     * A consumer-group runner that manages rebalances and persistent operator state.
     *
     * <p>Created by {@link ColumnarRunner#group}. As the consumer's rebalance
     * listener it restores each newly assigned logical partition from the state store
     * and saves each revoked partition back, so stateful operators survive rebalances
     * and clean restarts. The caller drives processing by calling
     * {@link #runOnce(Duration)} or {@link #runOnceTransactional(Duration)} in a
     * loop; the runner owns no threads.
     *
     * <p>Closing the runner saves and releases every partition it still owns and
     * closes the built topology. The consumer and producer remain the caller's to
     * close.
     */
    public static final class GroupRunner implements ConsumerRebalanceListener, AutoCloseable {
        private final BuiltColumnarTopology topology;
        private final Consumer<byte[], byte[]> consumer;
        private final Producer<byte[], byte[]> producer;
        private final ColumnarErrorPolicy errorPolicy;
        private final ColumnarStateStore stateStore;
        private final ColumnarMetrics metrics;
        private final Set<TopicPartition> owned = new HashSet<>();

        private GroupRunner(
                BuiltColumnarTopology topology,
                Consumer<byte[], byte[]> consumer,
                Producer<byte[], byte[]> producer,
                ColumnarErrorPolicy errorPolicy,
                ColumnarStateStore stateStore,
                ColumnarMetrics metrics) {
            this.topology = topology;
            this.consumer = consumer;
            this.producer = producer;
            this.errorPolicy = errorPolicy;
            this.stateStore = stateStore;
            this.metrics = metrics;
        }

        /**
         * Runs one fetch, process, produce, and commit cycle.
         *
         * <p>See {@link ColumnarRunner#runGroupOnce(BuiltColumnarTopology, Consumer,
         * Producer, Duration, ColumnarErrorPolicy, ColumnarMetrics)} for the cycle's
         * semantics; this method uses the runner's own topology, clients, policy, and
         * metrics.
         *
         * @param pollTimeout how long one poll may block
         * @return topic partition to next offset for everything committed this cycle;
         *     empty when the poll returned nothing
         */
        public Map<TopicPartition, Long> runOnce(Duration pollTimeout) {
            return runGroupOnce(topology, consumer, producer, pollTimeout, errorPolicy, metrics);
        }

        /**
         * Runs one exactly-once cycle in a producer transaction.
         *
         * <p>See {@link ColumnarRunner#runGroupOnceTransactional(BuiltColumnarTopology,
         * Consumer, Producer, Duration, ColumnarErrorPolicy, ColumnarMetrics)} for
         * the cycle's semantics; the runner's producer must be transactional and
         * already initialized with {@code initTransactions()}.
         *
         * @param pollTimeout how long one poll may block
         * @return topic partition to next offset for everything committed this cycle;
         *     empty when the poll returned nothing
         */
        public Map<TopicPartition, Long> runOnceTransactional(Duration pollTimeout) {
            return runGroupOnceTransactional(topology, consumer, producer, pollTimeout, errorPolicy, metrics);
        }

        /**
         * Returns the counters this runner records observations to.
         *
         * @return the metrics supplied when the runner was created
         */
        public ColumnarMetrics metrics() {
            return metrics;
        }

        /**
         * Restores newly assigned logical partitions from the state store.
         *
         * <p>Invoked by the consumer during rebalancing; do not call directly.
         *
         * @param partitions the topic partitions the consumer was assigned
         */
        @Override
        public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {
            var priorLogicalPartitions = owned.stream().map(TopicPartition::partition).collect(java.util.stream.Collectors.toSet());
            owned.addAll(partitions);
            partitions.stream()
                    .map(TopicPartition::partition)
                    .filter(partition -> !priorLogicalPartitions.contains(partition))
                    .distinct()
                    .forEach(partition -> topology.restorePartition(partition, stateStore.load(partition)));
        }

        /**
         * Saves and releases logical partitions the consumer is giving up.
         *
         * <p>Invoked by the consumer during rebalancing; do not call directly.
         *
         * @param partitions the topic partitions the consumer is revoking
         */
        @Override
        public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
            release(partitions, true);
        }

        /**
         * Releases lost logical partitions without saving their state.
         *
         * <p>Lost partitions may already be owned elsewhere, so their possibly stale
         * state is discarded rather than saved. Invoked by the consumer; do not call
         * directly.
         *
         * @param partitions the topic partitions the consumer lost
         */
        @Override
        public void onPartitionsLost(java.util.Collection<TopicPartition> partitions) {
            release(partitions, false);
        }

        /**
         * Saves every still-owned partition's state and closes the built topology.
         *
         * <p>The consumer and producer are not closed; they belong to the caller.
         */
        @Override
        public void close() {
            try {
                release(List.copyOf(owned), true);
            } finally {
                topology.close();
            }
        }

        private void release(java.util.Collection<TopicPartition> partitions, boolean save) {
            owned.removeAll(partitions);
            partitions.stream().map(TopicPartition::partition).distinct().forEach(partition -> {
                if (owned.stream().noneMatch(candidate -> candidate.partition() == partition)) {
                    if (save) {
                        stateStore.save(partition, topology.snapshotPartition(partition));
                    }
                    topology.releasePartition(partition);
                }
            });
        }
    }
}
