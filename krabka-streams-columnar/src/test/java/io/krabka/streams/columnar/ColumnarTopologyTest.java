package io.krabka.streams.columnar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

                assertEquals(List.of("out-a", "out-b"), first.stream().map(ProducedToTopic::topic).toList());
                assertEquals(2, second.size());
                try (var decoded = new ArrowIpcSerde(allocator).deserialize(first.get(0).record().value())) {
                    assertEquals(2, decoded.getRowCount());
                }
                assertEquals(List.of(), built.runBatch("other", input));
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
        }
    }
}
