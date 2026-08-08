package io.krabka.streams.columnar;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

class RowCodecTest {
    @Test
    void assemblesAndExplodesRows() {
        try (var allocator = new RootAllocator()) {
            var codec = new RowCodec<>(Serdes.String(), new JsonRowBridge<>(String.class), allocator);
            var records = List.of(
                    new ConsumedRecord(new byte[] {1}, "a".getBytes(StandardCharsets.UTF_8), 10, 0, 5),
                    new ConsumedRecord(new byte[] {2}, "b".getBytes(StandardCharsets.UTF_8), 11, 0, 6));

            try (var batch = codec.decode(records)) {
                assertEquals(2, batch.getRowCount());
                assertEquals(List.of("value", "__key", "__timestamp", "__partition", "__offset"),
                        batch.getSchema().getFields().stream().map(field -> field.getName()).toList());

                var output = codec.encode(batch);
                assertEquals(2, output.size());
                assertArrayEquals(new byte[] {1}, output.get(0).key());
                assertEquals("a", new String(output.get(0).value(), StandardCharsets.UTF_8));
                assertEquals(11, output.get(1).timestamp());
            }
        }
    }
}
