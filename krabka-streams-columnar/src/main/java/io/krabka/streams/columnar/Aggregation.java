package io.krabka.streams.columnar;

import java.util.Objects;

/** Defines one within-batch aggregation. */
public record Aggregation(String inputColumn, String outputColumn, AggregateFunction function) {
    public Aggregation {
        Objects.requireNonNull(inputColumn, "inputColumn");
        Objects.requireNonNull(outputColumn, "outputColumn");
        Objects.requireNonNull(function, "function");
    }
}
