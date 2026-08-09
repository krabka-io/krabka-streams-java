package io.krabka.streams.columnar;

import java.time.Duration;
import java.util.Objects;

/** Configuration for an inner, event-time, equi-join. */
public record ColumnarJoin(
        String leftKey,
        String rightKey,
        Duration window,
        String leftPrefix,
        String rightPrefix) {
    public ColumnarJoin(String leftKey, String rightKey, Duration window) {
        this(leftKey, rightKey, window, "left_", "right_");
    }

    public ColumnarJoin {
        Objects.requireNonNull(leftKey, "leftKey");
        Objects.requireNonNull(rightKey, "rightKey");
        Objects.requireNonNull(window, "window");
        Objects.requireNonNull(leftPrefix, "leftPrefix");
        Objects.requireNonNull(rightPrefix, "rightPrefix");
        if (window.isNegative()) {
            throw new IllegalArgumentException("window must not be negative");
        }
        if (!window.isZero() && window.toMillis() == 0) {
            throw new IllegalArgumentException("window must use millisecond precision");
        }
    }
}
