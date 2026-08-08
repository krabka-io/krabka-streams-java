package io.krabka.streams.test;

import io.krabka.streams.columnar.BuiltColumnarTopology;
import io.krabka.streams.columnar.ConsumedRecord;
import io.krabka.streams.columnar.ProduceRecord;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/** Runs a columnar topology without a broker. */
public final class ColumnarTestDriver {
    private final BuiltColumnarTopology topology;
    private final Map<String, ArrayDeque<ProduceRecord>> outputs = new HashMap<>();
    private final Map<TopicPartition, Long> nextOffsets = new HashMap<>();

    public ColumnarTestDriver(BuiltColumnarTopology topology) {
        this.topology = Objects.requireNonNull(topology, "topology");
    }

    /** Runs one record through the topology. Offsets start at zero for each topic partition. */
    public void pipeInput(String topic, int partition, byte[] key, byte[] value, long timestamp) {
        var topicPartition = new TopicPartition(topic, partition);
        long offset = nextOffsets.getOrDefault(topicPartition, 0L);
        nextOffsets.put(topicPartition, offset + 1L);
        pipeBatch(topic, List.of(new ConsumedRecord(key, value, timestamp, partition, offset)));
    }

    /** Runs one fetched partition batch through the topology. */
    public void pipeBatch(String topic, List<ConsumedRecord> records) {
        topology.runBatch(Objects.requireNonNull(topic, "topic"), List.copyOf(records)).forEach(output ->
                outputs.computeIfAbsent(output.topic(), ignored -> new ArrayDeque<>()).add(output.record()));
    }

    public boolean isOutputEmpty(String topic) {
        return outputSize(topic) == 0;
    }

    public int outputSize(String topic) {
        var queue = outputs.get(topic);
        return queue == null ? 0 : queue.size();
    }

    /** Reads the next record or throws if the topic has no output. */
    public ProduceRecord readOutput(String topic) {
        var queue = outputs.get(topic);
        if (queue == null || queue.isEmpty()) {
            throw new NoSuchElementException("topic `" + topic + "` has no output");
        }
        return queue.remove();
    }

    /** Removes and returns all records for a topic. */
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
