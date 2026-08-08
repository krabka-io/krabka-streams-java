package io.krabka.streams.columnar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

class BuiltinOpTest {
    @Test
    void filtersSelectsAndAddsColumns() {
        try (var allocator = new RootAllocator();
                var payload = ArrowTestData.transactions(
                        allocator, new String[] {"a", "a", "b"}, new long[] {5, 3, 9})) {
            var metadata = List.of(
                    new ArrowBatchSupport.RowMetadata(null, 1, 0, 0),
                    new ArrowBatchSupport.RowMetadata(null, 2, 0, 1),
                    new ArrowBatchSupport.RowMetadata(null, 3, 0, 2));
            try (var batch = ArrowBatchSupport.withMetadata(payload, metadata, allocator)) {
                var filtered = run(
                        BuiltinOp.filter(
                                allocator,
                                (root, row) -> ((BigIntVector) root.getVector("amount")).get(row) > 4),
                        batch);
                try (filtered) {
                    assertEquals(2, filtered.getRowCount());
                    assertEquals(BlobCodec.KEY_COLUMN, filtered.getVector(2).getName());

                    var selected = run(BuiltinOp.select(allocator, "user"), filtered);
                    try (selected) {
                        assertEquals(5, selected.getFieldVectors().size());
                        assertNull(selected.getVector("amount"));
                    }

                    var doubled = run(
                            BuiltinOp.withColumns(
                                    allocator,
                                    new DerivedColumn(
                                            new Field(
                                                    "double_amount",
                                                    FieldType.nullable(new ArrowType.Int(64, true)),
                                                    null),
                                            (root, row) -> ((BigIntVector) root.getVector("amount")).get(row) * 2)),
                            filtered);
                    try (doubled) {
                        assertEquals(10, ((BigIntVector) doubled.getVector("double_amount")).get(0));
                        assertEquals(18, ((BigIntVector) doubled.getVector("double_amount")).get(1));
                    }
                }
            }
        }
    }

    @Test
    void groupsAndAggregatesWithinOneBatch() {
        try (var allocator = new RootAllocator();
                var batch = ArrowTestData.transactions(
                        allocator, new String[] {"a", "a", "b"}, new long[] {5, 3, 9})) {
            var grouped = run(
                    BuiltinOp.groupBy(
                            allocator,
                            List.of("user"),
                            new Aggregation("amount", "total", AggregateFunction.SUM),
                            new Aggregation("amount", "count", AggregateFunction.COUNT)),
                    batch);
            try (grouped) {
                assertEquals(2, grouped.getRowCount());
                var users = (VarCharVector) grouped.getVector("user");
                var totals = (BigIntVector) grouped.getVector("total");
                var counts = (BigIntVector) grouped.getVector("count");
                assertEquals("a", users.getObject(0).toString());
                assertEquals(8, totals.get(0));
                assertEquals(2, counts.get(0));
                assertEquals("b", users.getObject(1).toString());
                assertEquals(9, totals.get(1));
            }
        }
    }

    private static org.apache.arrow.vector.VectorSchemaRoot run(
            BuiltinOp operator, org.apache.arrow.vector.VectorSchemaRoot batch) {
        var context = new ColumnarContext();
        operator.process(context, batch);
        return context.drain().get(0);
    }
}
