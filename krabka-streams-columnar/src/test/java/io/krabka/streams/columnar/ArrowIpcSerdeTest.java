package io.krabka.streams.columnar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.RootAllocator;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

class ArrowIpcSerdeTest {
    @Test
    void roundTripsArrowStream() {
        try (var allocator = new RootAllocator();
                var batch = ArrowTestData.transactions(allocator, new String[] {"a", "b"}, new long[] {1, 2})) {
            var serde = new ArrowIpcSerde(allocator);
            var bytes = serde.serializer().serialize("orders", batch);

            try (var decoded = serde.deserializer().deserialize("orders", bytes)) {
                assertEquals(batch.getSchema(), decoded.getSchema());
                assertEquals(2, decoded.getRowCount());
                assertEquals(2L, ((org.apache.arrow.vector.BigIntVector) decoded.getVector("amount")).get(1));
            }
        }
    }

    @Test
    void rejectsGarbage() {
        try (var allocator = new RootAllocator()) {
            var serde = new ArrowIpcSerde(allocator);
            assertThrows(
                    SerializationException.class,
                    () -> serde.deserializer().deserialize("orders", new byte[] {1, 2, 3}));
        }
    }
}
