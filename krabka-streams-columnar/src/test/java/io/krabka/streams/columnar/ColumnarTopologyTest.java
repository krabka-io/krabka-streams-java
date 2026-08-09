package io.krabka.streams.columnar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.junit.jupiter.api.Test;

class ColumnarTopologyTest {
    @Test
    void runsReusableTopologyAndFanOut() {
        try (var allocator = new RootAllocator()) {
            var codec = new BlobCodec(allocator);
            var topology = new ColumnarTopology(allocator);
            var source = topology.addSource("source", List.of("in"), codec);
            var filter = topology.addOperator(
                    "filter",
                    BuiltinOp.filter(
                            allocator,
                            (batch, row) -> ((BigIntVector) batch.getVector("amount")).get(row) > 4),
                    source);
            topology.addSink("first", "out-a", codec, filter);
            topology.addSink("second", "out-b", codec, filter);
            var built = topology.build();

            try (var payload = ArrowTestData.transactions(
                    allocator, new String[] {"a", "b", "c"}, new long[] {1, 5, 9})) {
                var input = List.of(new ConsumedRecord(
                        null, new ArrowIpcSerde(allocator).serialize(payload), 7, 0, 0));

                var first = built.runBatch("in", input);
                var second = built.runBatch("in", input);

                assertThat(first.stream().map(ProducedToTopic::topic).toList())
                        .usingRecursiveComparison()
                        .isEqualTo(List.of("out-a", "out-b"));
                assertEquals(2, second.size());
                try (var decoded = new ArrowIpcSerde(allocator).deserialize(first.get(0).record().value())) {
                    assertEquals(2, decoded.getRowCount());
                }
                assertThat(built.runBatch("other", input)).isEmpty();
            }
        }
    }

    @Test
    void validatesSourcesSinksAndNames() {
        try (var allocator = new RootAllocator()) {
            assertThrows(ColumnarException.class, () -> new ColumnarTopology(allocator).build());

            var topology = new ColumnarTopology(allocator);
            var source = topology.addSource("same", List.of("in"), new BlobCodec(allocator));
            topology.addSink("same", "out", new BlobCodec(allocator), source);
            assertThrows(ColumnarException.class, topology::build);

            var other = new ColumnarTopology(allocator);
            var foreignSource = other.addSource("foreign", List.of("in"), new BlobCodec(allocator));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> topology.addSink("foreign-sink", "out", new BlobCodec(allocator), foreignSource));
        }
    }

    @Test
    void mergesBranchesAndPassesSourceBytesThrough() {
        try (var allocator = new RootAllocator();
                var payload = ArrowTestData.transactions(allocator, new String[] {"a"}, new long[] {1})) {
            var codec = new BlobCodec(allocator);
            var topology = new ColumnarTopology(allocator);
            var left = topology.addSource("left", List.of("in-left"), codec);
            var right = topology.addSource("right", List.of("in-right"), codec);
            var merged = topology.addMerge("merge", List.of(left, right));
            topology.addSink("sink", "out", codec, merged);
            var bytes = new ArrowIpcSerde(allocator).serialize(payload);
            var output = topology.build().runBatches(java.util.Map.of(
                    "in-left", List.of(new ConsumedRecord(null, bytes, 1, 0, 0)),
                    "in-right", List.of(new ConsumedRecord(null, bytes, 1, 0, 0))));
            try (var result = new ArrowIpcSerde(allocator).deserialize(output.get(0).record().value())) {
                assertEquals(2, result.getRowCount());
            }

            var passthrough = new ColumnarTopology(allocator);
            var raw = passthrough.addSource("raw", List.of("raw-in"), codec);
            passthrough.addPassThroughSink("copy", "raw-out", raw);
            var malformed = new byte[] {1, 2, 3};
            var copied = passthrough.build().runBatch(
                    "raw-in", List.of(new ConsumedRecord(null, malformed, 4, 0, 0)));
            assertThat(copied.get(0).record().value()).containsExactly(malformed);
        }
    }
}
