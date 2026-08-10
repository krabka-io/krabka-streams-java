package io.krabka.streams.columnar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
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
            assertEquals(101, consumer.committed(java.util.Set.of(partition)).get(partition).offset());
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

    @Test
    @SuppressWarnings("deprecation")
    void groupRunnerSubscribesToAllSources() {
        try (var allocator = new RootAllocator();
                var consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            var topology = new ColumnarTopology(allocator);
            var source = topology.addSource("source", List.of("in-a", "in-b"), new BlobCodec(allocator));
            topology.addPassThroughSink("sink", "out", source);

            ColumnarRunner.group(topology, consumer, producer);

            assertEquals(java.util.Set.of("in-a", "in-b"), consumer.subscription());
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void groupRunnerLoadsAndSavesStateAtRebalanceBoundaries() {
        try (var allocator = new RootAllocator();
                var consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            var topology = new ColumnarTopology(allocator);
            var source = topology.addSource("source", List.of("in"), new BlobCodec(allocator));
            topology.addPassThroughSink("sink", "out", source);
            var stateStore = new RecordingStateStore();
            var runner = ColumnarRunner.group(
                    topology,
                    consumer,
                    producer,
                    ColumnarErrorPolicy.fail(),
                    stateStore,
                    new ColumnarMetrics());
            var partition = new TopicPartition("in", 3);

            runner.onPartitionsAssigned(List.of(partition));
            runner.onPartitionsRevoked(List.of(partition));

            assertThat(stateStore.loaded).usingRecursiveComparison().isEqualTo(List.of(3));
            assertThat(stateStore.saved).usingRecursiveComparison().isEqualTo(List.of(3));
            runner.close();
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void asynchronousSendCompletesExceptionallyOnBrokerError() {
        try (var producer = new MockProducer<byte[], byte[]>(
                false, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            var sent = ColumnarRunner.sendAsync(
                    List.of(new ProducedToTopic("out", new ProduceRecord(null, bytes("value"), 1))),
                    producer);
            producer.errorNext(new org.apache.kafka.common.KafkaException("send failed"));

            assertThatThrownBy(sent::join)
                    .isInstanceOf(java.util.concurrent.CompletionException.class)
                    .hasCauseInstanceOf(org.apache.kafka.common.KafkaException.class);
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void deadLettersFailedBatchesAndReportsMetrics() {
        try (var allocator = new RootAllocator();
                var payload = ArrowTestData.transactions(
                        allocator, new String[] {"a"}, new long[] {1});
                var consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            var codec = new BlobCodec(allocator);
            var topology = new ColumnarTopology(allocator);
            var source = topology.addSource("source", List.of("in"), codec);
            var failing = topology.addOperator("failing", FailingStatefulProcessor::new, source);
            topology.addSink("sink", "out", codec, failing);
            var partition = new TopicPartition("in", 0);
            consumer.assign(List.of(partition));
            consumer.updateBeginningOffsets(Map.of(partition, 0L));
            consumer.schedulePollTask(() -> {
                var record = new ConsumerRecord<byte[], byte[]>(
                        "in", 0, 0, bytes("k"), new ArrowIpcSerde(allocator).serialize(payload));
                record.headers().add("trace-id", bytes("abc"));
                consumer.addRecord(record);
            });
            var metrics = new ColumnarMetrics();

            var built = topology.build();
            var offsets = ColumnarRunner.runGroupOnce(
                    built,
                    consumer,
                    producer,
                    Duration.ZERO,
                    ColumnarErrorPolicy.deadLetter("dlq"),
                    metrics);

            assertThat(offsets).usingRecursiveComparison().isEqualTo(Map.of(partition, 1L));
            assertThat(producer.history()).hasSize(1);
            var deadLetter = producer.history().get(0);
            assertThat(deadLetter.topic()).isEqualTo("dlq");
            assertThat(deadLetter.value()).containsExactly(new ArrowIpcSerde(allocator).serialize(payload));
            assertThat(java.util.List.of(deadLetter.headers().lastHeader("trace-id").value()))
                    .usingRecursiveComparison()
                    .isEqualTo(List.of(bytes("abc")));
            assertThat(metrics.snapshot())
                    .usingRecursiveComparison()
                    .ignoringFields("processingNanos")
                    .isEqualTo(new ColumnarMetrics.Snapshot(1, 1, 0, 1, 1, 0));
            assertThat(metrics.snapshot().processingNanos()).isNotNegative();
            assertThat(built.snapshotPartition(0)).isEmpty();
            built.close();
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void rethrowsRetriableFailuresInsteadOfSkippingOrDeadLettering() {
        try (var allocator = new RootAllocator();
                var payload = ArrowTestData.transactions(
                        allocator, new String[] {"a"}, new long[] {1});
                var consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            var codec = new BlobCodec(allocator);
            var topology = new ColumnarTopology(allocator);
            var source = topology.addSource("source", List.of("in"), codec);
            var retriable = topology.addOperator(
                    "retriable",
                    () -> (context, batch) -> {
                        throw new org.apache.kafka.common.errors.TimeoutException("registry fetch pending");
                    },
                    source);
            topology.addSink("sink", "out", codec, retriable);
            var partition = new TopicPartition("in", 0);
            consumer.assign(List.of(partition));
            consumer.updateBeginningOffsets(Map.of(partition, 0L));
            consumer.schedulePollTask(() -> consumer.addRecord(new ConsumerRecord<>(
                    "in", 0, 0, null, new ArrowIpcSerde(allocator).serialize(payload))));
            var metrics = new ColumnarMetrics();

            var built = topology.build();
            assertThatThrownBy(() -> ColumnarRunner.runGroupOnce(
                            built,
                            consumer,
                            producer,
                            Duration.ZERO,
                            ColumnarErrorPolicy.deadLetter("dlq"),
                            metrics))
                    .isInstanceOf(org.apache.kafka.common.errors.TimeoutException.class);

            assertThat(producer.history()).isEmpty();
            assertThat(consumer.committed(java.util.Set.of(partition))).doesNotContainKey(partition);
            built.close();
        }
    }

    private static final class FailingStatefulProcessor implements StatefulColumnarProcessor {
        private int calls;

        @Override
        public void process(
                ColumnarContext context, org.apache.arrow.vector.VectorSchemaRoot batch) {
            calls++;
            throw new ColumnarException("broken batch");
        }

        @Override
        public byte[] snapshot() {
            return new byte[] {(byte) calls};
        }

        @Override
        public void restore(byte[] snapshot) {
            calls = Byte.toUnsignedInt(snapshot[0]);
        }
    }

    private static final class RecordingStateStore implements ColumnarStateStore {
        private final java.util.ArrayList<Integer> loaded = new java.util.ArrayList<>();
        private final java.util.ArrayList<Integer> saved = new java.util.ArrayList<>();

        @Override
        public Map<String, byte[]> load(int partition) {
            loaded.add(partition);
            return Map.of();
        }

        @Override
        public void save(int partition, Map<String, byte[]> snapshot) {
            saved.add(partition);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
