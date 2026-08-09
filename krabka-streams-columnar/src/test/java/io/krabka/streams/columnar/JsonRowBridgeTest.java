package io.krabka.streams.columnar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                assertThat(batch.getSchema().getFields().stream().map(field -> field.getName()).toList())
                        .usingRecursiveComparison()
                        .isEqualTo(List.of("id", "amount", "tags"));
                assertThat(bridge.batchToRows(batch)).usingRecursiveComparison().isEqualTo(rows);
            }
        }
    }

    @Test
    void keepsOneSchemaAcrossBatches() {
        try (var allocator = new RootAllocator()) {
            var bridge = new JsonRowBridge<>(Sample.class);
            try (var first = bridge.rowsToBatch(List.of(new Sample(null)), allocator);
                    var second = bridge.rowsToBatch(List.of(new Sample(7L)), allocator)) {
                assertThat(second.getSchema()).usingRecursiveComparison().isEqualTo(first.getSchema());
                assertThat(second.getVector("value").getObject(0).toString()).isEqualTo("7");
            }
        }
    }

    @Test
    void derivesStableRequiredFieldsFromJsonSchema() {
        var schema = "{\"type\":\"object\",\"required\":[\"id\"],\"properties\":{"
                + "\"id\":{\"type\":\"string\"},\"amount\":{\"type\":\"integer\"},"
                + "\"tags\":{\"type\":\"array\"}}}";
        try (var allocator = new RootAllocator()) {
            var bridge = JsonRowBridge.fromJsonSchema(Order.class, schema);
            var rows = List.of(new Order("a", 5, List.of("new")));
            try (var batch = bridge.rowsToBatch(rows, allocator)) {
                assertThat(batch.getSchema().getFields().stream().map(field -> field.isNullable()).toList())
                        .usingRecursiveComparison()
                        .isEqualTo(List.of(false, true, true));
                assertThat(bridge.batchToRows(batch)).usingRecursiveComparison().isEqualTo(rows);
            }
            assertThatThrownBy(() -> {
                        try (var ignored = bridge.rowsToBatch(
                                List.of(new Order(null, 1, List.of())), allocator)) {
                            assertThat(ignored.getRowCount()).isOne();
                        }
                    })
                    .isInstanceOf(ColumnarException.class)
                    .hasMessageContaining("required JSON field is null: id");
        }
    }

    @Test
    void enforcesScalarJsonSchemaNullability() {
        try (var allocator = new RootAllocator()) {
            var bridge = JsonRowBridge.fromJsonSchema(String.class, "{\"type\":\"string\"}");

            assertThatThrownBy(() -> bridge.rowsToBatch(
                            java.util.Collections.singletonList(null), allocator))
                    .isInstanceOf(ColumnarException.class)
                    .hasMessageContaining("required JSON field is null: value");
        }
    }

    record Order(String id, long amount, List<String> tags) {
    }

    record Sample(Long value) {
    }
}
