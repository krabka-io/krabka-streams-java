package io.krabka.streams.coordination;

import java.util.Objects;
import java.util.Optional;

/**
 * One decoded record of {@link CoordinationCodec#TOPIC}, with the offset it sits at.
 *
 * <p>{@link CoordinationTransport#readRoleRecords(Role)} returns these in offset order,
 * and {@link RoleStateBuilder} folds them. The offset is the join sequence of a
 * registration, because log compaction keeps the offset of every record it retains. The
 * succession rules rank candidates on that offset.
 *
 * <p>The value is empty for a tombstone. A tombstone of a registration key deregisters
 * one member, and a tombstone of a lease key clears the lease of one role.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * for (CoordinationEntry entry : transport.readRoleRecords(role)) {
 *     builder.apply(entry);
 * }
 * RoleState state = builder.build();
 * }</pre>
 *
 * @param offset the offset of the record in the partition of its role
 * @param key the decoded record key
 * @param value the decoded record value, and empty for a tombstone
 */
public record CoordinationEntry(long offset, CoordinationKey key,
        Optional<CoordinationValue> value) {
    /**
     * Rejects a null key and a null value holder.
     *
     * @param offset the offset of the record in the partition of its role
     * @param key the decoded record key
     * @param value the decoded record value, and empty for a tombstone
     */
    public CoordinationEntry {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
    }
}
