package io.krabka.streams.columnar;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/** Runs one fetch, process, produce, and flush cycle for a partition. */
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
        return runPartitionOnce(
                Objects.requireNonNull(topology, "topology").build(),
                consumer,
                producer,
                topic,
                partition,
                offset,
                pollTimeout);
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

        var records = new ArrayList<ConsumedRecord>(polled.size());
        long nextOffset = offset;
        for (var record : polled) {
            records.add(new ConsumedRecord(
                    record.key(),
                    record.value() == null ? new byte[0] : record.value(),
                    record.timestamp(),
                    record.partition(),
                    record.offset()));
            nextOffset = Math.max(nextOffset, Math.addExact(record.offset(), 1));
        }

        produce(topology.runBatch(topic, records), producer);
        producer.flush();
        consumer.commitSync(Map.of(topicPartition, new OffsetAndMetadata(nextOffset)));
        return nextOffset;
    }

    /** Polls and processes every partition assigned by the consumer group, then commits their offsets. */
    public static Map<TopicPartition, Long> runGroupOnce(
            BuiltColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer,
            Duration pollTimeout) {
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(producer, "producer");
        var offsets = processPoll(topology, consumer, producer, pollTimeout);
        producer.flush();
        if (!offsets.isEmpty()) {
            consumer.commitSync(offsets);
        }
        return nextOffsets(offsets);
    }

    /** Atomically produces a group poll and commits its consumed offsets in the producer transaction. */
    public static Map<TopicPartition, Long> runGroupOnceTransactional(
            BuiltColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer,
            Duration pollTimeout) {
        producer.beginTransaction();
        try {
            var offsets = processPoll(topology, consumer, producer, pollTimeout);
            if (!offsets.isEmpty()) {
                producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
            }
            producer.commitTransaction();
            return nextOffsets(offsets);
        } catch (RuntimeException error) {
            producer.abortTransaction();
            throw error;
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> processPoll(
            BuiltColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer,
            Duration pollTimeout) {
        var polled = consumer.poll(pollTimeout);
        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        var input = new HashMap<String, List<ConsumedRecord>>();
        for (var partition : polled.partitions()) {
            var records = input.computeIfAbsent(partition.topic(), ignored -> new ArrayList<>());
            for (var record : polled.records(partition)) {
                records.add(new ConsumedRecord(
                        record.key(),
                        record.value() == null ? new byte[0] : record.value(),
                        record.timestamp(),
                        record.partition(),
                        record.offset()));
            }
            offsets.put(partition, new OffsetAndMetadata(
                    Math.addExact(polled.records(partition).get(polled.records(partition).size() - 1).offset(), 1)));
        }
        produce(topology.runBatches(input), producer);
        return offsets;
    }

    /** Subscribes a group-managed consumer to every source topic in the topology. */
    public static void subscribe(ColumnarTopology topology, Consumer<byte[], byte[]> consumer) {
        consumer.subscribe(topology.sourceTopics());
    }

    /** Creates a reusable group runner and subscribes it to every source topic. */
    public static GroupRunner group(
            ColumnarTopology topology,
            Consumer<byte[], byte[]> consumer,
            Producer<byte[], byte[]> producer) {
        subscribe(topology, consumer);
        return new GroupRunner(topology.build(), consumer, producer);
    }

    private static void produce(List<ProducedToTopic> outputs, Producer<byte[], byte[]> producer) {
        for (var output : outputs) {
            var record = output.record();
            Long timestamp = record.timestamp() < 0 ? null : record.timestamp();
            producer.send(new ProducerRecord<byte[], byte[]>(
                    output.topic(), null, timestamp, record.key(), record.value()));
        }
    }

    private static Map<TopicPartition, Long> nextOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        return offsets.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey, entry -> entry.getValue().offset()));
    }

    public static final class GroupRunner {
        private final BuiltColumnarTopology topology;
        private final Consumer<byte[], byte[]> consumer;
        private final Producer<byte[], byte[]> producer;

        private GroupRunner(
                BuiltColumnarTopology topology,
                Consumer<byte[], byte[]> consumer,
                Producer<byte[], byte[]> producer) {
            this.topology = topology;
            this.consumer = consumer;
            this.producer = producer;
        }

        public Map<TopicPartition, Long> runOnce(Duration pollTimeout) {
            return runGroupOnce(topology, consumer, producer, pollTimeout);
        }

        public Map<TopicPartition, Long> runOnceTransactional(Duration pollTimeout) {
            return runGroupOnceTransactional(topology, consumer, producer, pollTimeout);
        }
    }
}
