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

/**
 * Creates the built-in Arrow batch operators.
 *
 * <p>Five operators cover the common transformations: {@link #filter filter},
 * {@link #select select}, {@link #withColumns withColumns},
 * {@link #groupBy groupBy}, and {@link #windowedGroupBy windowedGroupBy}. Every
 * factory takes the {@link BufferAllocator} that will own its output batches, and
 * every operator forwards exactly one batch per input batch.
 *
 * <p>Add an operator to a topology with
 * {@link ColumnarTopology#addOperator(String, BuiltinOp, ColumnarNode)}; the operator
 * is copied per logical partition there, so {@code groupBy} state never leaks across
 * partitions. The grouping operators are stateful and cumulative: they retain groups
 * across batches and participate in snapshot and restore.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var source = topology.addSource("source", List.of("transactions"), codec);
 * var large = topology.addOperator(
 *     "large",
 *     BuiltinOp.filter(allocator, (batch, row) ->
 *         ((BigIntVector) batch.getVector("amount")).get(row) > 4),
 *     source);
 * var totals = topology.addOperator(
 *     "totals",
 *     BuiltinOp.groupBy(
 *         allocator,
 *         List.of("user"),
 *         new Aggregation("amount", "total", AggregateFunction.SUM)),
 *     large);
 * topology.addSink("sink", "user-totals", codec, totals);
 * }</pre>
 */
public final class BuiltinOp implements StatefulColumnarProcessor {
    /** The window start column added by {@link #windowedGroupBy}, epoch milliseconds. */
    public static final String WINDOW_START_COLUMN = "__window_start";

    /** The window end column added by {@link #windowedGroupBy}, epoch milliseconds. */
    public static final String WINDOW_END_COLUMN = "__window_end";
    private final Supplier<Function<VectorSchemaRoot, VectorSchemaRoot>> operationFactory;
    private final Function<VectorSchemaRoot, VectorSchemaRoot> operation;

    private BuiltinOp(Supplier<Function<VectorSchemaRoot, VectorSchemaRoot>> operationFactory) {
        this.operationFactory = operationFactory;
        this.operation = operationFactory.get();
    }

    /**
     * Creates an operator that keeps the rows a predicate accepts.
     *
     * <p>The predicate runs once per row; passing rows are copied into a new batch
     * with the same schema, in their original order. Metadata columns travel with the
     * rows, so {@code __offset} still identifies the source record after filtering.
     *
     * @param allocator the allocator that owns the output batches
     * @param predicate the row predicate
     * @return the filter operator
     */
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

    /**
     * Creates an operator that projects the named payload columns.
     *
     * <p>The output keeps the named columns in the order given, then appends
     * whichever reserved metadata columns exist in the input — you do not list
     * metadata columns to keep them. Duplicate names are ignored after the first;
     * a name the input lacks makes the evaluation throw {@link ColumnarException}.
     *
     * @param allocator the allocator that owns the output batches
     * @param columns the payload columns to keep, in output order
     * @return the projection operator
     */
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

    /**
     * Creates an operator that adds or replaces derived columns.
     *
     * <p>A derived column whose name matches an existing column replaces it in place,
     * keeping its position; a new name is appended after the existing columns. Values
     * are coerced to each column's declared Arrow type with overflow checking;
     * {@code Utf8} accepts any value via {@code toString()}.
     *
     * @param allocator the allocator that owns the output batches
     * @param columns the columns to derive
     * @return the with-columns operator
     * @throws ColumnarException if a derived column uses a reserved metadata name
     */
    public static BuiltinOp withColumns(BufferAllocator allocator, DerivedColumn... columns) {
        Objects.requireNonNull(allocator, "allocator");
        var derived = List.copyOf(Arrays.asList(columns));
        ArrowBatchSupport.rejectReservedPayloadColumns(
                derived.stream().map(column -> column.field().getName()).toList());
        return new BuiltinOp(() -> batch -> withColumns(batch, derived, allocator));
    }

    /**
     * Creates a cumulative group-by operator.
     *
     * <p>Rows are grouped by the key columns' values and the groups are retained
     * across every batch processed by the same built topology, so each output batch
     * is the current cumulative result: key columns first, aggregate columns after,
     * groups ordered by first appearance. Metadata columns are dropped from the
     * output unless they are grouped by.
     *
     * <p>An unknown key or input column, or an empty key list, makes the evaluation
     * throw {@link ColumnarException}.
     *
     * @param allocator the allocator that owns the output batches
     * @param keys the payload columns to group by; at least one
     * @param aggregations the aggregate columns to compute
     * @return the group-by operator
     */
    public static BuiltinOp groupBy(
            BufferAllocator allocator, Collection<String> keys, Aggregation... aggregations) {
        Objects.requireNonNull(allocator, "allocator");
        var keyColumns = List.copyOf(keys);
        var aggregateColumns = List.copyOf(Arrays.asList(aggregations));
        return new BuiltinOp(() -> new GroupByOperation(
                keyColumns, aggregateColumns, null, null, allocator));
    }

    /**
     * Groups cumulatively into fixed event-time windows using the Kafka timestamp
     * column.
     *
     * <p>Each row is assigned to the fixed window containing its {@code __timestamp};
     * the output adds {@link #WINDOW_START_COLUMN} and {@link #WINDOW_END_COLUMN} as
     * epoch milliseconds after the key columns. Closed windows are retained for one
     * window size past stream time; use
     * {@link #windowedGroupBy(BufferAllocator, Collection, java.time.Duration, java.time.Duration, Aggregation...)}
     * for a longer retention.
     *
     * @param allocator the allocator that owns the output batches
     * @param keys the payload columns to group by; at least one
     * @param windowSize the fixed window size; at least one millisecond
     * @param aggregations the aggregate columns to compute
     * @return the windowed group-by operator
     * @throws IllegalArgumentException if the window is shorter than one millisecond
     */
    public static BuiltinOp windowedGroupBy(
            BufferAllocator allocator,
            Collection<String> keys,
            java.time.Duration windowSize,
            Aggregation... aggregations) {
        Objects.requireNonNull(allocator, "allocator");
        long windowMillis = Objects.requireNonNull(windowSize, "windowSize").toMillis();
        if (windowMillis < 1) {
            throw new IllegalArgumentException("windowSize must be at least one millisecond");
        }
        return windowedGroupBy(allocator, keys, windowSize, windowSize, aggregations);
    }

    /**
     * Groups into fixed event-time windows and retains closed windows for the
     * supplied duration.
     *
     * <p>Stream time is the largest {@code __timestamp} seen so far; windows whose
     * end falls more than the retention behind stream time are pruned from state, and
     * rows arriving for pruned windows are dropped. Use partition snapshots when
     * window state must survive a rebalance or restart.
     *
     * @param allocator the allocator that owns the output batches
     * @param keys the payload columns to group by; at least one
     * @param windowSize the fixed window size; at least one millisecond
     * @param retention how long closed windows accept late rows; at least the window size
     * @param aggregations the aggregate columns to compute
     * @return the windowed group-by operator
     * @throws IllegalArgumentException if the window is shorter than one millisecond
     *     or the retention is shorter than the window
     */
    public static BuiltinOp windowedGroupBy(
            BufferAllocator allocator,
            Collection<String> keys,
            java.time.Duration windowSize,
            java.time.Duration retention,
            Aggregation... aggregations) {
        Objects.requireNonNull(allocator, "allocator");
        var keyColumns = List.copyOf(keys);
        var aggregateColumns = List.copyOf(Arrays.asList(aggregations));
        long windowMillis = Objects.requireNonNull(windowSize, "windowSize").toMillis();
        long retentionMillis = Objects.requireNonNull(retention, "retention").toMillis();
        if (windowMillis < 1) {
            throw new IllegalArgumentException("windowSize must be at least one millisecond");
        }
        if (retentionMillis < windowMillis) {
            throw new IllegalArgumentException("retention must not be shorter than windowSize");
        }
        return new BuiltinOp(() -> new GroupByOperation(
                keyColumns, aggregateColumns, windowMillis, retentionMillis, allocator));
    }

    /**
     * Applies the operator and forwards exactly one output batch.
     *
     * @param context the collector the output batch is forwarded to
     * @param batch the input batch, owned by the framework
     */
    @Override
    public void process(ColumnarContext context, VectorSchemaRoot batch) {
        context.forward(operation.apply(batch));
    }

    /**
     * Serializes the operator's state.
     *
     * @return the grouping state for {@code groupBy} operators, or an empty array for
     *     the stateless operators
     */
    @Override
    public byte[] snapshot() {
        return operation instanceof SnapshotOperation stateful ? stateful.snapshot() : new byte[0];
    }

    /**
     * Replaces the operator's state with a previously taken snapshot.
     *
     * @param snapshot bytes previously returned by {@link #snapshot()}
     * @throws ColumnarException if the bytes cannot be restored or a non-empty
     *     snapshot is restored into a stateless operator
     */
    @Override
    public void restore(byte[] snapshot) {
        if (operation instanceof SnapshotOperation stateful) {
            stateful.restore(snapshot);
        } else if (snapshot.length != 0) {
            throw new ColumnarException("cannot restore state into a stateless operator");
        }
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
            Long windowMillis,
            long retainedAfter,
            BufferAllocator allocator) {
        if (keys.isEmpty()) {
            throw new ColumnarException("groupBy requires at least one key column");
        }
        var keyVectors = keys.stream().map(name -> requiredVector(batch, name)).toList();
        var inputVectors = aggregations.stream()
                .map(aggregation -> requiredVector(batch, aggregation.inputColumn()))
                .toList();
        var timestamps = windowMillis == null ? null : requiredVector(batch, ArrowBatchSupport.TIMESTAMP);
        for (int row = 0; row < batch.getRowCount(); row++) {
            var key = new ArrayList<Object>(keyVectors.size());
            for (var vector : keyVectors) {
                key.add(stableKey(ArrowBatchSupport.value(vector, row)));
            }
            if (windowMillis != null) {
                long timestamp = ((Number) Objects.requireNonNull(
                                ArrowBatchSupport.value(timestamps, row), "event timestamp"))
                        .longValue();
                long start = Math.multiplyExact(Math.floorDiv(timestamp, windowMillis), windowMillis);
                long end = Math.addExact(start, windowMillis);
                if (end < retainedAfter) {
                    continue;
                }
                key.add(start);
                key.add(end);
            }
            var stableKey = java.util.Collections.unmodifiableList(new ArrayList<>(key));
            var state = groups.computeIfAbsent(stableKey, ignored -> new AggregateState(aggregations.size()));
            for (int index = 0; index < aggregations.size(); index++) {
                state.add(
                        index,
                        aggregations.get(index).function(),
                        stableKey(ArrowBatchSupport.value(inputVectors.get(index), row)));
            }
        }

        var fields = new ArrayList<Field>();
        keyVectors.forEach(vector -> fields.add(vector.getField()));
        if (windowMillis != null) {
            if (batch.getVector(WINDOW_START_COLUMN) != null || batch.getVector(WINDOW_END_COLUMN) != null) {
                throw new ColumnarException("window output column already exists");
            }
            fields.add(new Field(
                    WINDOW_START_COLUMN, FieldType.notNullable(new ArrowType.Int(64, true)), null));
            fields.add(new Field(
                    WINDOW_END_COLUMN, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        }
        for (int index = 0; index < aggregations.size(); index++) {
            fields.add(aggregateField(aggregations.get(index), inputVectors.get(index).getField()));
        }
        var result = ArrowBatchSupport.create(fields, groups.size(), allocator);
        try {
            int row = 0;
            for (var entry : groups.entrySet()) {
                int outputKeys = keys.size() + (windowMillis == null ? 0 : 2);
                for (int keyIndex = 0; keyIndex < outputKeys; keyIndex++) {
                    ArrowBatchSupport.setValue(
                            result.getVector(keyIndex), row, restoredKey(entry.getKey().get(keyIndex)));
                }
                for (int aggregateIndex = 0; aggregateIndex < aggregations.size(); aggregateIndex++) {
                    ArrowBatchSupport.setValue(
                            result.getVector(outputKeys + aggregateIndex),
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

    private interface SnapshotOperation extends Function<VectorSchemaRoot, VectorSchemaRoot> {
        byte[] snapshot();

        void restore(byte[] snapshot);
    }

    private static final class GroupByOperation implements SnapshotOperation {
        private final List<String> keys;
        private final List<Aggregation> aggregations;
        private final Long windowMillis;
        private final Long retentionMillis;
        private final BufferAllocator allocator;
        private Map<List<Object>, AggregateState> groups = new LinkedHashMap<>();
        private long streamTime = Long.MIN_VALUE;

        private GroupByOperation(
                List<String> keys,
                List<Aggregation> aggregations,
                Long windowMillis,
                Long retentionMillis,
                BufferAllocator allocator) {
            this.keys = keys;
            this.aggregations = aggregations;
            this.windowMillis = windowMillis;
            this.retentionMillis = retentionMillis;
            this.allocator = allocator;
        }

        @Override
        public VectorSchemaRoot apply(VectorSchemaRoot batch) {
            if (windowMillis != null) {
                var timestamps = (org.apache.arrow.vector.BigIntVector)
                        requiredVector(batch, ArrowBatchSupport.TIMESTAMP);
                for (int row = 0; row < batch.getRowCount(); row++) {
                    if (!timestamps.isNull(row)) {
                        streamTime = Math.max(streamTime, timestamps.get(row));
                    }
                }
            }
            long retainedAfter = pruneExpired();
            return groupBy(
                    batch, keys, aggregations, groups, windowMillis, retainedAfter, allocator);
        }

        @Override
        public byte[] snapshot() {
            try {
                var bytes = new java.io.ByteArrayOutputStream();
                try (var output = new java.io.ObjectOutputStream(bytes)) {
                    output.writeLong(streamTime);
                    output.writeObject(groups);
                }
                return bytes.toByteArray();
            } catch (java.io.IOException error) {
                throw new ColumnarException("cannot snapshot groupBy state", error);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void restore(byte[] snapshot) {
            try (var input = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(snapshot))) {
                streamTime = input.readLong();
                groups = (Map<List<Object>, AggregateState>) input.readObject();
            } catch (java.io.IOException | ClassNotFoundException | ClassCastException error) {
                throw new ColumnarException("cannot restore groupBy state", error);
            }
        }

        private long pruneExpired() {
            if (windowMillis == null || streamTime == Long.MIN_VALUE) {
                return Long.MIN_VALUE;
            }
            long cutoff;
            try {
                cutoff = Math.subtractExact(streamTime, retentionMillis);
            } catch (ArithmeticException ignored) {
                cutoff = Long.MIN_VALUE;
            }
            int windowEndIndex = keys.size() + 1;
            final long retainedAfter = cutoff;
            groups.entrySet().removeIf(entry ->
                    ((Number) entry.getKey().get(windowEndIndex)).longValue() < retainedAfter);
            return retainedAfter;
        }
    }

    private static Object stableKey(Object value) {
        if (value instanceof java.nio.ByteBuffer buffer) {
            var copy = buffer.duplicate();
            var bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return new BinaryKey(bytes);
        }
        if (value instanceof List<?> values) {
            return values.stream().map(BuiltinOp::stableKey).toList();
        }
        if (value instanceof Map<?, ?> values) {
            var stable = new LinkedHashMap<Object, Object>();
            values.forEach((key, item) -> stable.put(stableKey(key), stableKey(item)));
            return stable;
        }
        return value;
    }

    private static Object restoredKey(Object value) {
        if (value instanceof BinaryKey binary) {
            return binary.value();
        }
        if (value instanceof List<?> values) {
            return values.stream().map(BuiltinOp::restoredKey).toList();
        }
        if (value instanceof Map<?, ?> values) {
            var restored = new LinkedHashMap<Object, Object>();
            values.forEach((key, item) -> restored.put(restoredKey(key), restoredKey(item)));
            return restored;
        }
        return value;
    }

    private static final class BinaryKey implements java.io.Serializable, Comparable<BinaryKey> {
        private static final long serialVersionUID = 1L;
        private final byte[] value;

        private BinaryKey(byte[] value) {
            this.value = value.clone();
        }

        private byte[] value() {
            return value.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof BinaryKey binary && java.util.Arrays.equals(value, binary.value);
        }

        @Override
        public int hashCode() {
            return java.util.Arrays.hashCode(value);
        }

        @Override
        public int compareTo(BinaryKey other) {
            return java.util.Arrays.compareUnsigned(value, other.value);
        }
    }

    private static final class AggregateState implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
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
            return restoredKey(values[index]);
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
