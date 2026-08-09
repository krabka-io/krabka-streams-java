package io.krabka.streams.columnar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/** Creates the built-in Arrow batch operators. */
public final class BuiltinOp implements ColumnarProcessor {
    private final Supplier<Function<VectorSchemaRoot, VectorSchemaRoot>> operationFactory;
    private final Function<VectorSchemaRoot, VectorSchemaRoot> operation;

    private BuiltinOp(Supplier<Function<VectorSchemaRoot, VectorSchemaRoot>> operationFactory) {
        this.operationFactory = operationFactory;
        this.operation = operationFactory.get();
    }

    public static BuiltinOp filter(BufferAllocator allocator, RowPredicate predicate) {
        Objects.requireNonNull(allocator, "allocator");
        Objects.requireNonNull(predicate, "predicate");
        return new BuiltinOp(() -> batch -> {
            var rows = new int[batch.getRowCount()];
            int count = 0;
            for (int row = 0; row < batch.getRowCount(); row++) {
                if (predicate.test(batch, row)) {
                    rows[count++] = row;
                }
            }
            return ArrowBatchSupport.copyRows(batch, Arrays.copyOf(rows, count), allocator);
        });
    }

    public static BuiltinOp select(BufferAllocator allocator, String... columns) {
        Objects.requireNonNull(allocator, "allocator");
        var requested = List.copyOf(Arrays.asList(columns));
        return new BuiltinOp(() -> batch -> {
            var selected = new ArrayList<String>(requested);
            for (var reserved : ArrowBatchSupport.RESERVED) {
                if (batch.getVector(reserved) != null) {
                    selected.add(reserved);
                }
            }
            return ArrowBatchSupport.select(batch, selected, allocator);
        });
    }

    public static BuiltinOp withColumns(BufferAllocator allocator, DerivedColumn... columns) {
        Objects.requireNonNull(allocator, "allocator");
        var derived = List.copyOf(Arrays.asList(columns));
        ArrowBatchSupport.rejectReservedPayloadColumns(
                derived.stream().map(column -> column.field().getName()).toList());
        return new BuiltinOp(() -> batch -> withColumns(batch, derived, allocator));
    }

    public static BuiltinOp groupBy(
            BufferAllocator allocator, Collection<String> keys, Aggregation... aggregations) {
        Objects.requireNonNull(allocator, "allocator");
        var keyColumns = List.copyOf(keys);
        var aggregateColumns = List.copyOf(Arrays.asList(aggregations));
        return new BuiltinOp(() -> {
            var groups = new LinkedHashMap<List<Object>, AggregateState>();
            return batch -> groupBy(batch, keyColumns, aggregateColumns, groups, allocator);
        });
    }

    @Override
    public void process(ColumnarContext context, VectorSchemaRoot batch) {
        context.forward(operation.apply(batch));
    }

    BuiltinOp fresh() {
        return new BuiltinOp(operationFactory);
    }

    private static VectorSchemaRoot withColumns(
            VectorSchemaRoot batch, List<DerivedColumn> derived, BufferAllocator allocator) {
        var byName = new LinkedHashMap<String, DerivedColumn>();
        derived.forEach(column -> byName.put(column.field().getName(), column));
        var fields = new ArrayList<Field>();
        for (var field : batch.getSchema().getFields()) {
            var replacement = byName.remove(field.getName());
            fields.add(replacement == null ? field : replacement.field());
        }
        byName.values().forEach(column -> fields.add(column.field()));

        var result = ArrowBatchSupport.create(fields, batch.getRowCount(), allocator);
        try {
            var expressions = derived.stream().collect(java.util.stream.Collectors.toMap(
                    column -> column.field().getName(), DerivedColumn::value, (first, second) -> second));
            for (var vector : result.getFieldVectors()) {
                var expression = expressions.get(vector.getName());
                var source = batch.getVector(vector.getName());
                for (int row = 0; row < batch.getRowCount(); row++) {
                    if (expression == null) {
                        vector.copyFromSafe(row, row, source);
                    } else {
                        ArrowBatchSupport.setValue(vector, row, expression.value(batch, row));
                    }
                }
            }
            ArrowBatchSupport.setValueCounts(result);
            return result;
        } catch (RuntimeException error) {
            result.close();
            throw error;
        }
    }

    private static VectorSchemaRoot groupBy(
            VectorSchemaRoot batch,
            List<String> keys,
            List<Aggregation> aggregations,
            Map<List<Object>, AggregateState> groups,
            BufferAllocator allocator) {
        if (keys.isEmpty()) {
            throw new ColumnarException("groupBy requires at least one key column");
        }
        var keyVectors = keys.stream().map(name -> requiredVector(batch, name)).toList();
        var inputVectors = aggregations.stream()
                .map(aggregation -> requiredVector(batch, aggregation.inputColumn()))
                .toList();
        for (int row = 0; row < batch.getRowCount(); row++) {
            var key = new ArrayList<Object>(keyVectors.size());
            for (var vector : keyVectors) {
                key.add(ArrowBatchSupport.value(vector, row));
            }
            var stableKey = java.util.Collections.unmodifiableList(new ArrayList<>(key));
            var state = groups.computeIfAbsent(stableKey, ignored -> new AggregateState(aggregations.size()));
            for (int index = 0; index < aggregations.size(); index++) {
                state.add(index, aggregations.get(index).function(), ArrowBatchSupport.value(inputVectors.get(index), row));
            }
        }

        var fields = new ArrayList<Field>();
        keyVectors.forEach(vector -> fields.add(vector.getField()));
        for (int index = 0; index < aggregations.size(); index++) {
            fields.add(aggregateField(aggregations.get(index), inputVectors.get(index).getField()));
        }
        var result = ArrowBatchSupport.create(fields, groups.size(), allocator);
        try {
            int row = 0;
            for (var entry : groups.entrySet()) {
                for (int keyIndex = 0; keyIndex < keys.size(); keyIndex++) {
                    ArrowBatchSupport.setValue(result.getVector(keyIndex), row, entry.getKey().get(keyIndex));
                }
                for (int aggregateIndex = 0; aggregateIndex < aggregations.size(); aggregateIndex++) {
                    ArrowBatchSupport.setValue(
                            result.getVector(keys.size() + aggregateIndex),
                            row,
                            entry.getValue().value(aggregateIndex));
                }
                row++;
            }
            ArrowBatchSupport.setValueCounts(result);
            return result;
        } catch (RuntimeException error) {
            result.close();
            throw error;
        }
    }

    private static FieldVector requiredVector(VectorSchemaRoot batch, String name) {
        var vector = batch.getVector(name);
        if (vector == null) {
            throw new ColumnarException("Arrow column does not exist: " + name);
        }
        return vector;
    }

    private static Field aggregateField(Aggregation aggregation, Field input) {
        ArrowType type = aggregation.outputType() != null
                ? aggregation.outputType()
                : switch (aggregation.function()) {
                    case COUNT -> new ArrowType.Int(64, true);
                    case SUM, MIN, MAX -> input.getType();
                };
        return new Field(aggregation.outputColumn(), FieldType.nullable(type), input.getChildren());
    }

    private static final class AggregateState {
        private final Object[] values;

        private AggregateState(int size) {
            values = new Object[size];
        }

        private void add(int index, AggregateFunction function, Object next) {
            switch (function) {
                case COUNT -> values[index] = values[index] == null
                        ? 1L
                        : Math.addExact(((Number) values[index]).longValue(), 1L);
                case SUM -> {
                    if (next instanceof Float || next instanceof Double) {
                        values[index] = (values[index] == null ? 0.0 : ((Number) values[index]).doubleValue())
                                + ((Number) next).doubleValue();
                    } else if (next instanceof BigDecimal decimal) {
                        values[index] = (values[index] == null ? BigDecimal.ZERO : (BigDecimal) values[index])
                                .add(decimal);
                    } else if (next instanceof Number number) {
                        var current = values[index] == null
                                ? java.math.BigInteger.ZERO
                                : (java.math.BigInteger) values[index];
                        values[index] = current.add(new java.math.BigInteger(number.toString()));
                    }
                }
                case MIN -> values[index] = compare(values[index], next, true);
                case MAX -> values[index] = compare(values[index], next, false);
                default -> throw new IllegalStateException("unknown aggregate function " + function);
            }
        }

        private Object value(int index) {
            return values[index];
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private static Object compare(Object current, Object next, boolean minimum) {
            if (next == null) {
                return current;
            }
            if (current == null) {
                return next;
            }
            int comparison = ((Comparable) current).compareTo(next);
            return minimum ? (comparison <= 0 ? current : next) : (comparison >= 0 ? current : next);
        }
    }
}
