package io.krabka.streams.columnar;

import java.util.Objects;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Defines one aggregation, with an optional explicit output type.
 *
 * <p>Without an explicit type, {@link AggregateFunction#COUNT} outputs a signed
 * 64-bit integer and the other functions keep the input column's type. Pass an
 * {@link ArrowType} to override, for example to widen a 32-bit sum column to 64 bits.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var wideTotal = new Aggregation(
 *     "amount", "total", AggregateFunction.SUM, new ArrowType.Int(64, true));
 * var op = BuiltinOp.groupBy(allocator, List.of("user"), wideTotal);
 * }</pre>
 *
 * @param inputColumn the payload column the function reads
 * @param outputColumn the name of the aggregate column in the output batch
 * @param function the aggregate function to apply
 * @param outputType the Arrow type of the output column, or null for the default
 */
public record Aggregation(
        String inputColumn, String outputColumn, AggregateFunction function, ArrowType outputType) {
    /**
     * Creates an aggregation with the function's default output type.
     *
     * @param inputColumn the payload column the function reads
     * @param outputColumn the name of the aggregate column in the output batch
     * @param function the aggregate function to apply
     */
    public Aggregation(String inputColumn, String outputColumn, AggregateFunction function) {
        this(inputColumn, outputColumn, function, null);
    }

    /**
     * Validates that everything but the output type is present.
     *
     * @param inputColumn the payload column the function reads
     * @param outputColumn the name of the aggregate column in the output batch
     * @param function the aggregate function to apply
     * @param outputType the Arrow type of the output column, or null for the default
     * @throws NullPointerException if {@code inputColumn}, {@code outputColumn}, or
     *     {@code function} is null
     */
    public Aggregation {
        Objects.requireNonNull(inputColumn, "inputColumn");
        Objects.requireNonNull(outputColumn, "outputColumn");
        Objects.requireNonNull(function, "function");
    }
}
