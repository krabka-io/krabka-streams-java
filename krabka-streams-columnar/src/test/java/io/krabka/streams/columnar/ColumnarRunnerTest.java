package io.krabka.streams.columnar;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

class ColumnarRunnerTest {
    @SuppressWarnings("deprecation")
    @Test
    void fetchesProcessesProducesFlushesAndAdvancesOffset() {
        try (var allocator = new RootAllocator();
                var payload = ArrowTestData.transactions(
                        allocator, new String[] {"a", "b", "c"}, new long[] {1, 5, 9});
                var consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            var codec = new BlobCodec(allocator);
            var topology = new ColumnarTopology(allocator);
            var source = topology.addSource("source", List.of("in"), codec);
            var filter = topology.addOperator(
                    "filter",
                    BuiltinOp.filter(
                            allocator,
                            (batch, row) -> ((BigIntVector) batch.getVector("amount")).get(row) > 4),
                    source);
            topology.addSink("sink", "out", codec, filter);

            var partition = new TopicPartition("in", 0);
            consumer.updateBeginningOffsets(Map.of(partition, 0L));
            consumer.schedulePollTask(() -> consumer.addRecord(new ConsumerRecord<>(
                    "in", 0, 100, null, new ArrowIpcSerde(allocator).serialize(payload))));

            long next = ColumnarRunner.runPartitionOnce(
                    topology, consumer, producer, "in", 0, 100, Duration.ZERO);

            assertEquals(101, next);
            assertEquals(1, producer.history().size());
            assertEquals("out", producer.history().get(0).topic());
            try (var result = new ArrowIpcSerde(allocator).deserialize(producer.history().get(0).value())) {
                assertEquals(2, result.getRowCount());
            }
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    void emptyPollKeepsOffsetAndDoesNotProduce() {
        try (var allocator = new RootAllocator();
                var consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            var topology = new ColumnarTopology(allocator);
            var source = topology.addSource("source", List.of("in"), new BlobCodec(allocator));
            topology.addSink("sink", "out", new BlobCodec(allocator), source);
            consumer.updateBeginningOffsets(Map.of(new TopicPartition("in", 0), 42L));

            assertEquals(42, ColumnarRunner.runPartitionOnce(
                    topology, consumer, producer, "in", 0, 42, Duration.ZERO));
            assertEquals(0, producer.history().size());
        }
    }
}
