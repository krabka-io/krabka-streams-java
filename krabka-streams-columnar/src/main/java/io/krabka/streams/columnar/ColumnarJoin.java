package io.krabka.streams.columnar;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for an inner, event-time, equi-join.
 *
 * <p>Passed to {@link ColumnarTopology#addJoin(String, ColumnarJoin, ColumnarNode, ColumnarNode)}.
 * Two rows match when their key column values are equal and their {@code __timestamp}
 * values differ by at most the window. The joined batch carries the left and right
 * payload columns renamed with the prefixes, followed by the metadata columns of the
 * left row.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var join = new ColumnarJoin("order_id", "order_id", Duration.ofMinutes(5));
 * var joined = topology.addJoin("orders-with-payments", join, orders, payments);
 * }</pre>
 *
 * @param leftKey the join key column in the left branch's batches
 * @param rightKey the join key column in the right branch's batches
 * @param window the maximum event-time distance between matching rows; zero joins
 *     only equal timestamps
 * @param leftPrefix the prefix for left payload columns in the joined batch
 * @param rightPrefix the prefix for right payload columns in the joined batch
 */
public record ColumnarJoin(
        String leftKey,
        String rightKey,
        Duration window,
        String leftPrefix,
        String rightPrefix) {
    /**
     * Creates a join with the default {@code left_} and {@code right_} prefixes.
     *
     * @param leftKey the join key column in the left branch's batches
     * @param rightKey the join key column in the right branch's batches
     * @param window the maximum event-time distance between matching rows
     */
    public ColumnarJoin(String leftKey, String rightKey, Duration window) {
        this(leftKey, rightKey, window, "left_", "right_");
    }

    /**
     * Validates the configuration.
     *
     * @param leftKey the join key column in the left branch's batches
     * @param rightKey the join key column in the right branch's batches
     * @param window the maximum event-time distance between matching rows
     * @param leftPrefix the prefix for left payload columns in the joined batch
     * @param rightPrefix the prefix for right payload columns in the joined batch
     * @throws NullPointerException if any component is null
     * @throws IllegalArgumentException if the window is negative or below one
     *     millisecond of precision
     */
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
