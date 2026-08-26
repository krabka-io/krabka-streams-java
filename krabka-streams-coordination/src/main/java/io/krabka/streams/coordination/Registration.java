package io.krabka.streams.coordination;

import java.util.Objects;

/**
 * One member of a role announces that it is available.
 *
 * <p>A member writes a registration when it joins, and it writes a tombstone on the
 * same key when it leaves. The record carries no authority. A candidate holds no epoch
 * when it announces itself, and it must take no epoch to do so, so the append sits
 * outside a transaction.
 *
 * <p>The offset that the broker assigns to the record is the join sequence of the
 * member. Log compaction keeps the offset of every record it retains, so the succession
 * rules rank candidates on that offset. See {@link RoleState}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * Registration joined = new Registration(MemberId.of("node-1"), clock.nowMillis());
 * byte[] value = CoordinationCodec.encodeValue(joined);
 * }</pre>
 *
 * @param member the member that registered
 * @param registeredAt the time the member registered, in milliseconds since the Unix
 *     epoch
 */
public record Registration(MemberId member, long registeredAt) implements CoordinationValue {
    /**
     * Rejects a null member.
     *
     * @param member the member that registered
     * @param registeredAt the time the member registered, in milliseconds since the
     *     Unix epoch
     */
    public Registration {
        Objects.requireNonNull(member, "member");
    }

    /**
     * Returns the kind of the key that a registration sits under.
     *
     * @return {@link RecordKind#REGISTRATION}
     */
    @Override
    public RecordKind kind() {
        return RecordKind.REGISTRATION;
    }
}
