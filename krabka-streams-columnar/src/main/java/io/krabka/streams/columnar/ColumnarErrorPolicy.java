package io.krabka.streams.columnar;

import java.util.Objects;

/** Selects how a group runner handles a failed logical partition batch. */
public record ColumnarErrorPolicy(Action action, String deadLetterTopic) {
    public enum Action {
        FAIL,
        SKIP,
        DEAD_LETTER
    }

    public ColumnarErrorPolicy {
        Objects.requireNonNull(action, "action");
        if (action == Action.DEAD_LETTER) {
            Objects.requireNonNull(deadLetterTopic, "deadLetterTopic");
        } else if (deadLetterTopic != null) {
            throw new IllegalArgumentException("deadLetterTopic requires DEAD_LETTER");
        }
    }

    public static ColumnarErrorPolicy fail() {
        return new ColumnarErrorPolicy(Action.FAIL, null);
    }

    public static ColumnarErrorPolicy skip() {
        return new ColumnarErrorPolicy(Action.SKIP, null);
    }

    public static ColumnarErrorPolicy deadLetter(String topic) {
        return new ColumnarErrorPolicy(Action.DEAD_LETTER, topic);
    }
}
