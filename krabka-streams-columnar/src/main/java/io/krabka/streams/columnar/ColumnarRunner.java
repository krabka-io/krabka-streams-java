package io.krabka.streams.columnar;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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

        for (var output : topology.build().runBatch(topic, records)) {
            var record = output.record();
            Long timestamp = record.timestamp() < 0 ? null : record.timestamp();
            producer.send(new ProducerRecord<byte[], byte[]>(
                    output.topic(), null, timestamp, record.key(), record.value()));
        }
        producer.flush();
        return nextOffset;
    }
}
