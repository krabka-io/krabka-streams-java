package io.krabka.streams.columnar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.krabka.streams.columnar.barrier.BarrierAlignment;
import io.krabka.streams.columnar.barrier.BarrierCut;
import io.krabka.streams.columnar.barrier.BarrierCutDecoder;
import io.krabka.streams.columnar.barrier.BarrierCutReader;
import io.krabka.streams.columnar.barrier.BarrierCutStatus;
import io.krabka.streams.columnar.barrier.BarrierListener;
import io.krabka.streams.columnar.barrier.BarrierTestRecords;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

class ColumnarBarrierRunnerTest {
    private static final String GROUP = "ledger";
    private static final TopicPartition IN_0 = new TopicPartition("in", 0);
    private static final TopicPartition IN_1 = new TopicPartition("in", 1);
    private static final TopicPartition STATE_0 = new TopicPartition(BarrierCutDecoder.TOPIC, 0);

    @Test
    void alignsOnTheCutSnapshotsTheEpochAndCommitsTheCutOffsets() {
        try (var allocator = new RootAllocator();
                var consumer = new MockConsumer<byte[], byte[]>("earliest");
                var cutConsumer = new MockConsumer<byte[], byte[]>("earliest");
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            publishCut(cutConsumer, 7, Map.of(IN_0, 2L, IN_1, 1L));
            var stateStore = new RecordingStateStore();
            var fired = new ArrayList<BarrierCut>();
            var runner = start(allocator, consumer, cutConsumer, producer, stateStore, fired::add);
            assign(consumer, IN_0, IN_1);
            runner.onPartitionsAssigned(List.of(IN_0, IN_1));
            addRows(consumer, allocator, IN_0, 0, 4);
            addRows(consumer, allocator, IN_1, 0, 2);

            runner.runOnce(Duration.ZERO);

            assertThat(fired)
                    .usingRecursiveComparison()
                    .isEqualTo(List.of(cut(7, Map.of(IN_0, 2L, IN_1, 1L))));
            assertThat(runner.pendingCut()).isEmpty();
            assertThat(committed(consumer, IN_0, IN_1))
                    .usingRecursiveComparison()
                    .isEqualTo(Map.of(IN_0, 2L, IN_1, 1L));
            assertThat(stateStore.saves)
                    .usingRecursiveComparison()
                    .isEqualTo(List.of(new Save(0, 7), new Save(1, 7)));
            assertThat(rowCounts(producer, allocator)).isEqualTo(List.of(2, 1));
            runner.close();
        }
    }

    @Test
    void waitsUntilEveryAssignedPartitionReachesTheCut() {
        try (var allocator = new RootAllocator();
                var consumer = new MockConsumer<byte[], byte[]>("earliest");
                var cutConsumer = new MockConsumer<byte[], byte[]>("earliest");
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            publishCut(cutConsumer, 7, Map.of(IN_0, 2L, IN_1, 1L));
            var stateStore = new RecordingStateStore();
            var fired = new ArrayList<BarrierCut>();
            var runner = start(allocator, consumer, cutConsumer, producer, stateStore, fired::add);
            assign(consumer, IN_0, IN_1);
            runner.onPartitionsAssigned(List.of(IN_0, IN_1));
            addRows(consumer, allocator, IN_0, 0, 4);

            runner.runOnce(Duration.ZERO);

            assertThat(fired).isEmpty();
            assertThat(runner.pendingCut()).get().usingRecursiveComparison().isEqualTo(cut(7, Map.of(IN_0, 2L, IN_1, 1L)));
            assertThat(stateStore.saves).isEmpty();
            assertThat(committed(consumer, IN_0)).usingRecursiveComparison().isEqualTo(Map.of(IN_0, 2L));
            assertThat(consumer.position(IN_0)).isEqualTo(2L);

            addRows(consumer, allocator, IN_1, 0, 2);
            runner.runOnce(Duration.ZERO);

            assertThat(fired).usingRecursiveComparison().isEqualTo(List.of(cut(7, Map.of(IN_0, 2L, IN_1, 1L))));
            assertThat(stateStore.saves)
                    .usingRecursiveComparison()
                    .isEqualTo(List.of(new Save(0, 7), new Save(1, 7)));
            runner.close();
        }
    }

    @Test
    void commitsTheBarrierInsideTheProducerTransaction() {
        try (var allocator = new RootAllocator();
                var consumer = new MockConsumer<byte[], byte[]>("earliest");
                var cutConsumer = new MockConsumer<byte[], byte[]>("earliest");
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            producer.initTransactions();
            publishCut(cutConsumer, 3, Map.of(IN_0, 2L));
            var stateStore = new RecordingStateStore();
            var fired = new ArrayList<BarrierCut>();
            var runner = start(allocator, consumer, cutConsumer, producer, stateStore, fired::add);
            assign(consumer, IN_0);
            runner.onPartitionsAssigned(List.of(IN_0));
            addRows(consumer, allocator, IN_0, 0, 4);

            runner.runOnceTransactional(Duration.ZERO);

            assertThat(producer.transactionCommitted()).isTrue();
            assertThat(producer.consumerGroupOffsetsHistory()).hasSize(1);
            assertThat(List.copyOf(producer.consumerGroupOffsetsHistory().get(0).values()))
                    .usingRecursiveComparison()
                    .isEqualTo(List.of(Map.of(IN_0, new OffsetAndMetadata(2L))));
            assertThat(fired).usingRecursiveComparison().isEqualTo(List.of(cut(3, Map.of(IN_0, 2L))));
            assertThat(stateStore.saves).usingRecursiveComparison().isEqualTo(List.of(new Save(0, 3)));
            runner.close();
        }
    }

    @Test
    void restoresToAnEpochAndSeeksEveryInputToTheCut() {
        try (var allocator = new RootAllocator();
                var consumer = new MockConsumer<byte[], byte[]>("earliest");
                var cutConsumer = new MockConsumer<byte[], byte[]>("earliest");
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            publishCut(cutConsumer, 7, Map.of(IN_0, 2L, IN_1, 5L));
            var stateStore = new RecordingStateStore();
            var runner = start(allocator, consumer, cutConsumer, producer, stateStore, ignored -> { });
            assign(consumer, IN_0, IN_1);
            runner.onPartitionsAssigned(List.of(IN_0, IN_1));

            var restored = runner.restoreToEpoch(7);

            assertThat(restored).usingRecursiveComparison().isEqualTo(cut(7, Map.of(IN_0, 2L, IN_1, 5L)));
            assertThat(stateStore.loads)
                    .usingRecursiveComparison()
                    .isEqualTo(List.of(
                            new Save(0, ColumnarStateStore.LIVE_EPOCH),
                            new Save(1, ColumnarStateStore.LIVE_EPOCH),
                            new Save(0, 7),
                            new Save(1, 7)));
            assertThat(consumer.position(IN_0)).isEqualTo(2L);
            assertThat(consumer.position(IN_1)).isEqualTo(5L);
            runner.close();
        }
    }

    @Test
    void restoresToTheLatestCompleteCut() {
        try (var allocator = new RootAllocator();
                var consumer = new MockConsumer<byte[], byte[]>("earliest");
                var cutConsumer = new MockConsumer<byte[], byte[]>("earliest");
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            publishCuts(
                    cutConsumer,
                    cutRecord(0, 7, BarrierCutStatus.COMPLETE, Map.of(IN_0, 2L)),
                    cutRecord(1, 8, BarrierCutStatus.PARTIAL, Map.of(IN_0, 9L)));
            var runner = start(
                    allocator, consumer, cutConsumer, producer, new RecordingStateStore(), ignored -> { });
            assign(consumer, IN_0);
            runner.onPartitionsAssigned(List.of(IN_0));

            assertThat(runner.restoreToLatestCut())
                    .get()
                    .usingRecursiveComparison()
                    .isEqualTo(cut(7, Map.of(IN_0, 2L)));
            assertThat(consumer.position(IN_0)).isEqualTo(2L);
            runner.close();
        }
    }

    @Test
    void reportsAnUnknownEpochAndAMissingAlignment() {
        try (var allocator = new RootAllocator();
                var consumer = new MockConsumer<byte[], byte[]>("earliest");
                var cutConsumer = new MockConsumer<byte[], byte[]>("earliest");
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            publishCut(cutConsumer, 7, Map.of(IN_0, 2L));
            var runner = start(
                    allocator, consumer, cutConsumer, producer, new RecordingStateStore(), ignored -> { });
            assign(consumer, IN_0);

            assertThatThrownBy(() -> runner.restoreToEpoch(9))
                    .isInstanceOf(ColumnarException.class)
                    .hasMessage("no complete barrier cut for group ledger epoch 9");
            assertThatThrownBy(() -> runner.restoreToEpoch(-1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("barrier epoch must not be negative");
            runner.close();

            var plain = ColumnarRunner.group(topology(allocator), consumer, producer);
            assertThatThrownBy(plain::restoreToLatestCut)
                    .isInstanceOf(ColumnarException.class)
                    .hasMessage("this runner has no barrier alignment");
            plain.close();
        }
    }

    @Test
    void reportsAvailableSnapshotsWhenACutWasReclaimed() {
        try (var allocator = new RootAllocator();
                var consumer = new MockConsumer<byte[], byte[]>("earliest");
                var cutConsumer = new MockConsumer<byte[], byte[]>("earliest");
                var producer = new MockProducer<byte[], byte[]>(
                        true, null, new ByteArraySerializer(), new ByteArraySerializer())) {
            publishCut(cutConsumer, 9, Map.of(IN_0, 2L));
            var stateStore = new RecordingStateStore() {
                @Override
                public java.util.Optional<List<Long>> retainedEpochs(int partition) {
                    return java.util.Optional.of(List.of(7L, 8L));
                }
            };
            var runner = start(allocator, consumer, cutConsumer, producer, stateStore, ignored -> { });
            assign(consumer, IN_0);

            assertThatThrownBy(() -> runner.restoreToEpoch(9))
                    .isInstanceOf(ColumnarException.class)
                    .hasMessage("snapshot epoch 9 is not retained; available epochs by partition: {0=[7, 8]}");
            runner.close();
        }
    }

    private static ColumnarRunner.GroupRunner start(
            RootAllocator allocator,
            MockConsumer<byte[], byte[]> consumer,
            MockConsumer<byte[], byte[]> cutConsumer,
            MockProducer<byte[], byte[]> producer,
            ColumnarStateStore stateStore,
            BarrierListener listener) {
        return ColumnarRunner.group(
                topology(allocator),
                consumer,
                producer,
                ColumnarErrorPolicy.fail(),
                stateStore,
                new ColumnarMetrics(),
                BarrierAlignment.on(GROUP, new BarrierCutReader(cutConsumer, Duration.ZERO))
                        .withListener(listener));
    }

    private static ColumnarTopology topology(RootAllocator allocator) {
        var codec = new BlobCodec(allocator);
        var topology = new ColumnarTopology(allocator);
        var source = topology.addSource("source", List.of("in"), codec);
        var counting = topology.addOperator("counting", CountingProcessor::new, source);
        topology.addSink("sink", "out", codec, counting);
        return topology;
    }

    private static void assign(MockConsumer<byte[], byte[]> consumer, TopicPartition... partitions) {
        consumer.rebalance(List.of(partitions));
        var beginning = new LinkedHashMap<TopicPartition, Long>();
        for (var partition : partitions) {
            beginning.put(partition, 0L);
        }
        consumer.updateBeginningOffsets(beginning);
    }

    private static void addRows(
            MockConsumer<byte[], byte[]> consumer,
            RootAllocator allocator,
            TopicPartition partition,
            long firstOffset,
            int count) {
        var serde = new ArrowIpcSerde(allocator);
        for (int index = 0; index < count; index++) {
            try (var payload = ArrowTestData.transactions(
                    allocator, new String[] {"user-" + index}, new long[] {index})) {
                consumer.addRecord(new ConsumerRecord<>(
                        partition.topic(),
                        partition.partition(),
                        firstOffset + index,
                        null,
                        serde.serialize(payload)));
            }
        }
    }

    private static void publishCut(
            MockConsumer<byte[], byte[]> cutConsumer, long epoch, Map<TopicPartition, Long> offsets) {
        publishCuts(cutConsumer, cutRecord(0, epoch, BarrierCutStatus.COMPLETE, offsets));
    }

    @SafeVarargs
    private static void publishCuts(
            MockConsumer<byte[], byte[]> cutConsumer, ConsumerRecord<byte[], byte[]>... records) {
        cutConsumer.updatePartitions(
                BarrierCutDecoder.TOPIC,
                List.of(new PartitionInfo(BarrierCutDecoder.TOPIC, 0, null, new Node[0], new Node[0])));
        cutConsumer.assign(List.of(STATE_0));
        cutConsumer.updateBeginningOffsets(Map.of(STATE_0, 0L));
        for (var record : records) {
            cutConsumer.addRecord(record);
        }
        cutConsumer.updateEndOffsets(Map.of(STATE_0, (long) records.length));
    }

    private static ConsumerRecord<byte[], byte[]> cutRecord(
            long offset, long epoch, BarrierCutStatus status, Map<TopicPartition, Long> offsets) {
        return new ConsumerRecord<>(
                BarrierCutDecoder.TOPIC,
                0,
                offset,
                BarrierTestRecords.key(BarrierTestRecords.CUT_KIND, GROUP, epoch),
                BarrierTestRecords.cutValue(epoch * 10, epoch * 10 + 1, status, offsets, List.of()));
    }

    private static BarrierCut cut(long epoch, Map<TopicPartition, Long> offsets) {
        return new BarrierCut(
                GROUP, epoch, epoch * 10, epoch * 10 + 1, BarrierCutStatus.COMPLETE, offsets, Set.of());
    }

    private static Map<TopicPartition, Long> committed(
            MockConsumer<byte[], byte[]> consumer, TopicPartition... partitions) {
        var result = new LinkedHashMap<TopicPartition, Long>();
        consumer.committed(Set.of(partitions))
                .forEach((partition, offset) -> result.put(partition, offset.offset()));
        return result;
    }

    private static List<Integer> rowCounts(MockProducer<byte[], byte[]> producer, RootAllocator allocator) {
        var serde = new ArrowIpcSerde(allocator);
        var counts = new ArrayList<Integer>();
        for (var record : producer.history()) {
            try (var batch = serde.deserialize(record.value())) {
                counts.add(batch.getRowCount());
            }
        }
        return counts;
    }

    private record Save(int partition, long epoch) {
    }

    private static final class CountingProcessor implements StatefulColumnarProcessor {
        private long rows;

        @Override
        public void process(ColumnarContext context, VectorSchemaRoot batch) {
            rows += batch.getRowCount();
            context.forward(batch);
        }

        @Override
        public byte[] snapshot() {
            return java.nio.ByteBuffer.allocate(Long.BYTES).putLong(rows).array();
        }

        @Override
        public void restore(byte[] snapshot) {
            rows = java.nio.ByteBuffer.wrap(snapshot).getLong();
        }
    }

    private static class RecordingStateStore implements ColumnarStateStore {
        private final List<Save> loads = new ArrayList<>();
        private final List<Save> saves = new ArrayList<>();
        private final Map<Save, Map<String, byte[]>> stored = new LinkedHashMap<>();

        @Override
        public Map<String, byte[]> load(int partition, long epoch) {
            loads.add(new Save(partition, epoch));
            return stored.getOrDefault(new Save(partition, epoch), Map.of());
        }

        @Override
        public void save(int partition, long epoch, Map<String, byte[]> snapshot) {
            saves.add(new Save(partition, epoch));
            stored.put(new Save(partition, epoch), snapshot);
        }
    }
}
