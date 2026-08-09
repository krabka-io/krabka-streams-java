package io.krabka.streams.columnar;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

class JsonRowBridgeTest {
    @Test
    void roundTripsRecordsAndNestedJson() {
        try (var allocator = new RootAllocator()) {
            var bridge = new JsonRowBridge<>(Order.class);
            var rows = List.of(
                    new Order("a", 5, List.of("new", "paid")),
                    new Order("b", 7, List.of("new")));

            try (var batch = bridge.rowsToBatch(rows, allocator)) {
                assertEquals(List.of("id", "amount", "tags"),
                        batch.getSchema().getFields().stream().map(field -> field.getName()).toList());
                assertEquals(rows, bridge.batchToRows(batch));
            }
        }
    }

    @Test
    void keepsOneSchemaAcrossBatches() {
        try (var allocator = new RootAllocator()) {
            var bridge = new JsonRowBridge<>(Sample.class);
            try (var first = bridge.rowsToBatch(List.of(new Sample(null)), allocator);
                    var second = bridge.rowsToBatch(List.of(new Sample(7L)), allocator)) {
                assertEquals(first.getSchema(), second.getSchema());
                assertEquals("7", second.getVector("value").getObject(0).toString());
            }
        }
    }

    record Order(String id, long amount, List<String> tags) {
    }

    record Sample(Long value) {
    }
}
