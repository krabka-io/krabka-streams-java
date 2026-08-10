package io.krabka.streams.columnar;

import java.util.Objects;

/**
 * Selects how a group runner handles a failed logical partition batch.
 *
 * <p>When processing one partition's poll fails, the runner first rolls that
 * partition's operator state back to its pre-batch snapshot, then applies the policy:
 * {@link Action#FAIL} rethrows, {@link Action#SKIP} commits past the poisoned batch
 * without producing output for it, and {@link Action#DEAD_LETTER} additionally
 * forwards each input record to the dead-letter topic with {@code krabka.error.*} and
 * {@code krabka.source.*} headers describing the failure and origin.
 *
 * <p>A {@link org.apache.kafka.common.errors.RetriableException} — thrown directly
 * or anywhere in the failure's cause chain, as with the schema cache's
 * {@code SchemaFetchPendingException} wrapped by a serde — is always rethrown
 * regardless of the policy: a transient condition clears on retry, so skipping or
 * dead-lettering the batch would discard healthy records.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var runner = ColumnarRunner.group(
 *     topology, consumer, producer,
 *     ColumnarErrorPolicy.deadLetter("transactions-dead-letter"),
 *     ColumnarStateStore.none(),
 *     new ColumnarMetrics());
 * }</pre>
 *
 * @param action what to do with the failed batch
 * @param deadLetterTopic the topic failed records are forwarded to; non-null exactly
 *     when the action is {@link Action#DEAD_LETTER}
 */
public record ColumnarErrorPolicy(Action action, String deadLetterTopic) {
    /** The reaction to a failed partition batch. */
    public enum Action {
        /** Rethrow the failure; nothing is produced or committed. */
        FAIL,

        /** Drop the batch's output and commit past it. */
        SKIP,

        /** Forward the batch's input records to the dead-letter topic, then commit. */
        DEAD_LETTER
    }

    /**
     * Validates the action and dead-letter topic combination.
     *
     * @param action what to do with the failed batch
     * @param deadLetterTopic the topic failed records are forwarded to, or null
     * @throws NullPointerException if {@code action} is null, or the action is
     *     {@link Action#DEAD_LETTER} and {@code deadLetterTopic} is null
     * @throws IllegalArgumentException if a dead-letter topic is supplied for another
     *     action
     */
    public ColumnarErrorPolicy {
        Objects.requireNonNull(action, "action");
        if (action == Action.DEAD_LETTER) {
            Objects.requireNonNull(deadLetterTopic, "deadLetterTopic");
        } else if (deadLetterTopic != null) {
            throw new IllegalArgumentException("deadLetterTopic requires DEAD_LETTER");
        }
    }

    /**
     * Returns the policy that rethrows failures.
     *
     * @return the fail-fast policy
     */
    public static ColumnarErrorPolicy fail() {
        return new ColumnarErrorPolicy(Action.FAIL, null);
    }

    /**
     * Returns the policy that skips failed batches.
     *
     * @return the skip policy
     */
    public static ColumnarErrorPolicy skip() {
        return new ColumnarErrorPolicy(Action.SKIP, null);
    }

    /**
     * Returns the policy that forwards failed batches to a dead-letter topic.
     *
     * @param topic the dead-letter topic
     * @return the dead-letter policy
     */
    public static ColumnarErrorPolicy deadLetter(String topic) {
        return new ColumnarErrorPolicy(Action.DEAD_LETTER, topic);
    }
}
