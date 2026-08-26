package io.krabka.streams.coordination;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Bounds checks that {@link Role} and {@link MemberId} share. */
final class Names {
    private Names() {
    }

    /**
     * Rejects an empty name and a name past its byte bound.
     *
     * @param name the name to check
     * @param field the field name the message reports
     * @param bound the maximum length in bytes
     * @throws CoordinationException if the name is empty or too long
     */
    static void check(String name, String field, int bound) {
        Objects.requireNonNull(name, field);
        if (name.isEmpty()) {
            throw new CoordinationException("a coordination " + field + " must not be empty");
        }
        int length = name.getBytes(StandardCharsets.UTF_8).length;
        if (length > bound) {
            throw new CoordinationException("a coordination " + field + " of " + length
                    + " bytes is longer than the " + bound + "-byte maximum");
        }
    }
}
