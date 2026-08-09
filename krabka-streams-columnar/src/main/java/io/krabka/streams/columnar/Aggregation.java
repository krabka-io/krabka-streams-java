package io.krabka.streams.columnar;

import java.util.Objects;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Defines one aggregation, with an optional explicit output type. */
public record Aggregation(
        String inputColumn, String outputColumn, AggregateFunction function, ArrowType outputType) {
    public Aggregation(String inputColumn, String outputColumn, AggregateFunction function) {
        this(inputColumn, outputColumn, function, null);
    }

    public Aggregation {
        Objects.requireNonNull(inputColumn, "inputColumn");
        Objects.requireNonNull(outputColumn, "outputColumn");
        Objects.requireNonNull(function, "function");
    }
}
