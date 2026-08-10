package io.krabka.streams.columnar;

/**
 * Built-in within-batch aggregate functions.
 *
 * <p>Used inside an {@link Aggregation} passed to
 * {@link BuiltinOp#groupBy(org.apache.arrow.memory.BufferAllocator, java.util.Collection, Aggregation...)}
 * or a windowed variant. Integral accumulation is exact and overflow-checked; an
 * overflow throws {@link ArithmeticException} rather than wrapping.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var totals = BuiltinOp.groupBy(
 *     allocator,
 *     List.of("user"),
 *     new Aggregation("amount", "total", AggregateFunction.SUM),
 *     new Aggregation("amount", "count", AggregateFunction.COUNT));
 * }</pre>
 */
public enum AggregateFunction {
    /**
     * Counts the rows in the group, nulls included. The output column is a signed
     * 64-bit integer unless the aggregation declares another type.
     */
    COUNT,

    /**
     * Sums the non-null input values. Integral inputs accumulate exactly, floating
     * point inputs as {@code double}, and decimal inputs as {@code BigDecimal}. An
     * all-null group yields null. The output keeps the input column's type unless the
     * aggregation declares another type.
     */
    SUM,

    /**
     * Keeps the smallest non-null input value, compared with {@link Comparable}. An
     * all-null group yields null.
     */
    MIN,

    /**
     * Keeps the largest non-null input value, compared with {@link Comparable}. An
     * all-null group yields null.
     */
    MAX
}
