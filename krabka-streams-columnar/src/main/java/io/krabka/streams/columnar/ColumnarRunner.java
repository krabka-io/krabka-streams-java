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

/** Runs fetch, process, asynchronously produce, and commit cycles. */
public final class ColumnarRunner {
    private ColumnarRunner() {
    }

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

    /** Polls and processes every partition assigned by the consumer group, then commits their offsets. */
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

    /** Atomically produces a group poll and commits its consumed offsets in the producer transaction. */
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

    /** Sends all records concurrently and completes when every broker acknowledgement arrives. */
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

    /** Subscribes a group-managed consumer to every source topic in the topology. */
    public static void subscribe(ColumnarTopology topology, Consumer<byte[], byte[]> consumer) {
        consumer.subscribe(topology.sourceTopics());
    }

    /** Creates an ephemeral, fail-fast group runner. */
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

    /** Creates a group runner with explicit failure, snapshot, and metric handling. */
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
                    if (errorPolicy.action() == ColumnarErrorPolicy.Action.FAIL) {
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

        public Map<TopicPartition, Long> runOnce(Duration pollTimeout) {
            return runGroupOnce(topology, consumer, producer, pollTimeout, errorPolicy, metrics);
        }

        public Map<TopicPartition, Long> runOnceTransactional(Duration pollTimeout) {
            return runGroupOnceTransactional(topology, consumer, producer, pollTimeout, errorPolicy, metrics);
        }

        public ColumnarMetrics metrics() {
            return metrics;
        }

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

        @Override
        public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
            release(partitions, true);
        }

        @Override
        public void onPartitionsLost(java.util.Collection<TopicPartition> partitions) {
            release(partitions, false);
        }

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
