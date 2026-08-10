package io.krabka.streams.test;

import io.krabka.streams.columnar.BuiltColumnarTopology;
import io.krabka.streams.columnar.ConsumedRecord;
import io.krabka.streams.columnar.ProduceRecord;
import io.krabka.streams.columnar.RecordHeader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Runs a columnar topology without a broker.
 *
 * <p>The driver mirrors Kafka's {@code TopologyTestDriver} for the columnar execution
 * model: records piped in are processed synchronously on the calling thread, and sink
 * output is captured per topic in FIFO order. Because the driver wraps one
 * {@link BuiltColumnarTopology}, stateful operators accumulate across calls exactly as
 * they would across polls in production.
 *
 * <p>The driver does not close the topology; close the {@code BuiltColumnarTopology}
 * (and its allocator) yourself, ideally with try-with-resources in the test.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try (var built = topology.build()) {
 *     var driver = new ColumnarTestDriver(built);
 *     driver.pipeInput("input", 0, key, value, 0L);
 *
 *     var output = driver.readOutput("output");
 *     assertThat(driver.isOutputEmpty("output")).isTrue();
 * }
 * }</pre>
 */
public final class ColumnarTestDriver {
    private final BuiltColumnarTopology topology;
    private final Map<String, ArrayDeque<ProduceRecord>> outputs = new HashMap<>();
    private final Map<TopicPartition, Long> nextOffsets = new HashMap<>();
    private final ArrayDeque<RuntimeException> faults = new ArrayDeque<>();

    /**
     * Creates a driver around a built topology.
     *
     * @param topology the topology to feed; the driver does not close it
     */
    public ColumnarTestDriver(BuiltColumnarTopology topology) {
        this.topology = Objects.requireNonNull(topology, "topology");
    }

    /**
     * Runs one record through the topology. Offsets start at zero for each topic
     * partition.
     *
     * @param topic the source topic to pipe the record into
     * @param partition the source partition number
     * @param key the record key, or null for a keyless record
     * @param value the record value
     * @param timestamp the record timestamp in epoch milliseconds
     */
    public void pipeInput(String topic, int partition, byte[] key, byte[] value, long timestamp) {
        pipeInput(topic, partition, key, value, timestamp, List.of());
    }

    /**
     * Runs one record, including immutable Kafka headers, through the topology.
     *
     * @param topic the source topic to pipe the record into
     * @param partition the source partition number
     * @param key the record key, or null for a keyless record
     * @param value the record value
     * @param timestamp the record timestamp in epoch milliseconds
     * @param headers the ordered Kafka headers to attach
     */
    public void pipeInput(
            String topic,
            int partition,
            byte[] key,
            byte[] value,
            long timestamp,
            List<RecordHeader> headers) {
        var topicPartition = new TopicPartition(topic, partition);
        long offset = nextOffsets.getOrDefault(topicPartition, 0L);
        pipeBatch(topic, List.of(new ConsumedRecord(key, value, timestamp, partition, offset, headers)));
        nextOffsets.put(topicPartition, offset + 1L);
    }

    /**
     * Runs one fetched partition batch through the topology.
     *
     * <p>All records are evaluated as one graph pass, which is how a production runner
     * hands a poll's worth of records to the topology. Sink output is appended to the
     * per-topic queues read by {@link #readOutput(String)}.
     *
     * @param topic the source topic the records were fetched from
     * @param records the records of the batch, all from the same topic
     * @throws RuntimeException the fault registered with {@link #failNext(RuntimeException)},
     *     if one is pending
     */
    public void pipeBatch(String topic, List<ConsumedRecord> records) {
        if (!faults.isEmpty()) {
            throw faults.remove();
        }
        topology.runBatch(Objects.requireNonNull(topic, "topic"), List.copyOf(records)).forEach(output ->
                outputs.computeIfAbsent(output.topic(), ignored -> new ArrayDeque<>()).add(output.record()));
    }

    /**
     * Throws the supplied fault before the next batch evaluation, once.
     *
     * <p>Registered faults are queued: each piped batch consumes one fault. Use this
     * to test error policies and retry paths without a failing codec or operator.
     *
     * @param fault the exception the next {@code pipeInput} or {@code pipeBatch} call throws
     */
    public void failNext(RuntimeException fault) {
        faults.add(Objects.requireNonNull(fault, "fault"));
    }

    /**
     * Returns whether a topic has no unread output.
     *
     * @param topic the sink topic to check
     * @return true when every record produced to the topic has been read
     */
    public boolean isOutputEmpty(String topic) {
        return outputSize(topic) == 0;
    }

    /**
     * Returns the number of unread records for a topic.
     *
     * @param topic the sink topic to check
     * @return the number of records available to {@link #readOutput(String)}
     */
    public int outputSize(String topic) {
        var queue = outputs.get(topic);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Reads the next record or throws if the topic has no output.
     *
     * @param topic the sink topic to read from
     * @return the oldest unread record produced to the topic
     * @throws NoSuchElementException if the topic has no unread output
     */
    public ProduceRecord readOutput(String topic) {
        var queue = outputs.get(topic);
        if (queue == null || queue.isEmpty()) {
            throw new NoSuchElementException("topic `" + topic + "` has no output");
        }
        return queue.remove();
    }

    /**
     * Removes and returns all records for a topic.
     *
     * @param topic the sink topic to drain
     * @return every unread record in production order; empty when there is none
     */
    public List<ProduceRecord> drainOutput(String topic) {
        var drained = new ArrayList<ProduceRecord>();
        while (!isOutputEmpty(topic)) {
            drained.add(readOutput(topic));
        }
        return List.copyOf(drained);
    }

    private record TopicPartition(String topic, int partition) {
        private TopicPartition {
            Objects.requireNonNull(topic, "topic");
        }
    }
}
