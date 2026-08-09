package io.krabka.streams.columnar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.testing.junit.testparameterinjector.junit5.TestParameterInjectorTest;
import com.google.testing.junit.testparameterinjector.junit5.TestParameters;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ColumnarStatefulFeaturesTest {
    @TestParameterInjectorTest
    @TestParameters("{windowMillis: 10, expectedWindows: 2, expectedTotal: 7}")
    @TestParameters("{windowMillis: 20, expectedWindows: 1, expectedTotal: 10}")
    void windowsAndSnapshotsRetainEventTimeState(
            long windowMillis, int expectedWindows, long expectedTotal) {
        try (var allocator = new RootAllocator();
                var payload = ArrowTestData.transactions(
                        allocator, new String[] {"a", "a"}, new long[] {5, 3});
                var batch = ArrowBatchSupport.withMetadata(
                        payload,
                        List.of(
                                new ArrowBatchSupport.RowMetadata(null, 1, 0, 0),
                                new ArrowBatchSupport.RowMetadata(null, 12, 0, 1)),
                        allocator)) {
            var operator = BuiltinOp.windowedGroupBy(
                    allocator,
                    List.of("user"),
                    Duration.ofMillis(windowMillis),
                    new Aggregation("amount", "total", AggregateFunction.SUM));
            try (var initial = run(operator, batch)) {
                assertThat(initial.getRowCount()).isEqualTo(expectedWindows);
            }

            var restored = BuiltinOp.windowedGroupBy(
                    allocator,
                    List.of("user"),
                    Duration.ofMillis(windowMillis),
                    new Aggregation("amount", "total", AggregateFunction.SUM));
            restored.restore(operator.snapshot());
            try (var nextPayload = ArrowTestData.transactions(
                            allocator, new String[] {"a"}, new long[] {2});
                    var next = ArrowBatchSupport.withMetadata(
                            nextPayload,
                            List.of(new ArrowBatchSupport.RowMetadata(null, 2, 0, 2)),
                            allocator);
                    var result = run(restored, next)) {
                assertThat(((BigIntVector) result.getVector("total")).get(0)).isEqualTo(expectedTotal);
                assertThat(((BigIntVector) result.getVector(BuiltinOp.WINDOW_START_COLUMN)).get(0))
                    .isZero();
            }
            try (var farPayload = ArrowTestData.transactions(
                            allocator, new String[] {"a"}, new long[] {1});
                    var far = ArrowBatchSupport.withMetadata(
                            farPayload,
                            List.of(new ArrowBatchSupport.RowMetadata(null, 100, 0, 3)),
                            allocator);
                    var advanced = run(restored, far)) {
                assertThat(advanced.getRowCount()).isOne();
            }
            try (var retainedPayload = ArrowTestData.transactions(
                            allocator, new String[] {"a"}, new long[] {1});
                    var retained = ArrowBatchSupport.withMetadata(
                            retainedPayload,
                            List.of(new ArrowBatchSupport.RowMetadata(null, 101, 0, 4)),
                            allocator);
                    var result = run(restored, retained)) {
                assertThat(result.getRowCount()).isOne();
            }
        }
    }

    @Test
    void joinsAcrossBatchesAndRestoresOnlyTheMatchingPartition() {
        try (var allocator = new RootAllocator()) {
            var topology = new ColumnarTopology(allocator);
            var codec = new BlobCodec(allocator);
            var left = topology.addSource("left", List.of("left"), codec);
            var right = topology.addSource("right", List.of("right"), codec);
            var joined = topology.addJoin(
                    "join", new ColumnarJoin("user", "user", Duration.ofMillis(10)), left, right);
            topology.addSink("sink", "out", codec, joined);

            Map<String, byte[]> snapshot;
            try (var built = topology.build();
                    var leftPayload = ArrowTestData.transactions(
                            allocator, new String[] {"a"}, new long[] {5})) {
                var leftInput = new ConsumedRecord(
                        bytes("key"), new ArrowIpcSerde(allocator).serialize(leftPayload), 100, 0, 0);
                assertThat(built.runBatch("left", List.of(leftInput))).isEmpty();
                snapshot = built.snapshotPartition(0);
            }

            try (var restored = topology.build();
                    var rightPayload = ArrowTestData.transactions(
                            allocator, new String[] {"a"}, new long[] {9})) {
                restored.restorePartition(0, snapshot);
                var serialized = new ArrowIpcSerde(allocator).serialize(rightPayload);
                assertThat(restored.runBatch(
                                "right", List.of(new ConsumedRecord(null, serialized, 105, 1, 0))))
                        .isEmpty();
                var output = restored.runBatch(
                        "right", List.of(new ConsumedRecord(null, serialized, 105, 0, 0)));
                assertThat(output).hasSize(1);
                try (var result = new ArrowIpcSerde(allocator).deserialize(output.get(0).record().value())) {
                    assertThat(result.getSchema().getFields().stream().map(field -> field.getName()).toList())
                            .usingRecursiveComparison()
                            .isEqualTo(List.of("left_user", "left_amount", "right_user", "right_amount"));
                    assertThat(((BigIntVector) result.getVector("left_amount")).get(0)).isEqualTo(5);
                    assertThat(((BigIntVector) result.getVector("right_amount")).get(0)).isEqualTo(9);
                }
            }
        }
    }

    @Test
    void gzipAndHeadersRoundTripThroughAnExistingCodec() {
        try (var allocator = new RootAllocator()) {
            var raw = new RowCodec<>(Serdes.String(), new JsonRowBridge<>(String.class), allocator);
            var gzip = new GzipBatchCodec(raw);
            var header = new RecordHeader("trace-id", bytes("abc"));
            var input = new ConsumedRecord(bytes("k"), bytes("hello"), 7, 0, 4, List.of(header));
            try (var decoded = raw.decode("in", List.of(input))) {
                var compressed = gzip.encode("compressed", decoded).get(0);
                assertThatThrownBy(() -> new GzipBatchCodec(raw, 4).decode(
                                "compressed",
                                List.of(new ConsumedRecord(
                                        compressed.key(), compressed.value(), 7, 0, 4, compressed.headers()))))
                        .isInstanceOf(ColumnarException.class)
                        .hasMessageContaining("maxUncompressedBytes=4");
                try (var inflated = gzip.decode(
                        "compressed",
                        List.of(new ConsumedRecord(
                                compressed.key(),
                                compressed.value(),
                                compressed.timestamp(),
                                0,
                                4,
                                compressed.headers())))) {
                    assertThat(raw.encode("out", inflated).get(0))
                            .usingRecursiveComparison()
                            .isEqualTo(new ProduceRecord(bytes("k"), bytes("hello"), 7, List.of(header)));
                }
            }
        }
    }

    @Test
    void fileStateStoreAtomicallyRoundTripsSnapshots(@TempDir java.nio.file.Path directory) {
        var store = new FileColumnarStateStore(directory);
        var expected = Map.of("aggregate", new byte[] {1, 2, 3});

        store.save(4, expected);

        assertThat(store.load(4)).usingRecursiveComparison().isEqualTo(expected);
        assertThat(store.load(5)).isEmpty();
    }

    private static org.apache.arrow.vector.VectorSchemaRoot run(
            BuiltinOp operator, org.apache.arrow.vector.VectorSchemaRoot batch) {
        var context = new ColumnarContext();
        operator.process(context, batch);
        return context.drain().get(0);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
