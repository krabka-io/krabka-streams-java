package io.krabka.streams.columnar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.junit.jupiter.api.Test;

class BlobCodecTest {
    @Test
    void stacksRecordsAndAddsMetadata() {
        try (var allocator = new RootAllocator();
                var first = ArrowTestData.transactions(allocator, new String[] {"a", "a"}, new long[] {1, 2});
                var second = ArrowTestData.transactions(allocator, new String[] {"b"}, new long[] {3})) {
            var serde = new ArrowIpcSerde(allocator);
            var codec = new BlobCodec(allocator);
            var records = List.of(
                    new ConsumedRecord(null, serde.serialize(first), 10, 0, 5),
                    new ConsumedRecord(new byte[] {7}, serde.serialize(second), 11, 0, 6));

            try (var decoded = codec.decode(records)) {
                assertEquals(3, decoded.getRowCount());
                assertEquals(6L, ((BigIntVector) decoded.getVector(BlobCodec.OFFSET_COLUMN)).get(2));
                assertEquals(11L, ((BigIntVector) decoded.getVector(BlobCodec.TIMESTAMP_COLUMN)).get(2));

                var output = codec.encode(decoded);
                assertEquals(2, output.size());
                assertEquals(10L, output.get(0).timestamp());
                assertEquals(11L, output.get(1).timestamp());
                try (var firstPayload = serde.deserialize(output.get(0).value());
                        var secondPayload = serde.deserialize(output.get(1).value())) {
                    assertEquals(2, firstPayload.getRowCount());
                    assertEquals(1, secondPayload.getRowCount());
                    assertEquals(2, firstPayload.getFieldVectors().size());
                }
            }
        }
    }

    @Test
    void escapesReservedPayloadAndRejectsEmptyInput() {
        try (var allocator = new RootAllocator()) {
            var codec = new BlobCodec(allocator);
            assertThrows(ColumnarException.class, () -> codec.decode(List.of()));
            try (var bad = ArrowBatchSupport.create(
                    List.of(new org.apache.arrow.vector.types.pojo.Field(
                            BlobCodec.KEY_COLUMN,
                            org.apache.arrow.vector.types.pojo.FieldType.nullable(
                                    new org.apache.arrow.vector.types.pojo.ArrowType.Binary()),
                            null)),
                    1,
                    allocator)) {
                ArrowBatchSupport.setValueCounts(bad);
                var bytes = new ArrowIpcSerde(allocator).serialize(bad);
                try (var decoded = codec.decode(List.of(new ConsumedRecord(null, bytes, 0, 0, 0)))) {
                    assertTrue(decoded.getVector(BlobCodec.payloadColumn(BlobCodec.KEY_COLUMN)) != null);
                    var output = codec.encode(decoded);
                    try (var restored = new ArrowIpcSerde(allocator).deserialize(output.get(0).value())) {
                        assertTrue(restored.getVector(BlobCodec.KEY_COLUMN) != null);
                    }
                }
            }
        }
    }

    @Test
    void splitsOutputAtSoftCap() {
        try (var allocator = new RootAllocator()) {
            var users = new String[64];
            var amounts = new long[64];
            for (int row = 0; row < users.length; row++) {
                users[row] = "user-" + row + "-" + "x".repeat(32);
                amounts[row] = row;
            }
            try (var payload = ArrowTestData.transactions(allocator, users, amounts)) {
                var ipc = new ArrowIpcSerde(allocator).serialize(payload);
                var codec = new BlobCodec(allocator, 1_024);
                try (var decoded = codec.decode(List.of(new ConsumedRecord(null, ipc, 0, 0, 0)))) {
                    var output = codec.encode(decoded);
                    assertTrue(output.size() > 1);
                    assertTrue(output.stream().allMatch(record -> record.value().length <= 1_024));
                }
            }
        }
    }

    @Test
    void rejectsASingleRowOverTheHardCap() {
        try (var allocator = new RootAllocator();
                var payload = ArrowTestData.transactions(allocator, new String[] {"too-large"}, new long[] {1})) {
            var codec = new BlobCodec(allocator, 1);
            var bytes = new ArrowIpcSerde(allocator).serialize(payload);
            try (var decoded = codec.decode(List.of(new ConsumedRecord(null, bytes, 0, 0, 0)))) {
                assertThrows(ColumnarException.class, () -> codec.encode(decoded));
            }
        }
    }
}
