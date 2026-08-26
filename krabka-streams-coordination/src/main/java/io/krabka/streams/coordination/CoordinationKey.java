package io.krabka.streams.coordination;

import java.util.Objects;
import java.util.Optional;

/**
 * The decoded key of one record of {@link CoordinationCodec#TOPIC}.
 *
 * <p>The key is the compaction key, so the topic keeps the last record of every role
 * and every member. The key also carries the record kind, so one topic holds both
 * kinds and a reader picks the value layout from the key it already read.
 *
 * <p>The member is present for {@link RecordKind#REGISTRATION} and absent for
 * {@link RecordKind#LEASE}. A lease belongs to the role and not to one member, and the
 * frozen layout writes the empty string in that position.
 * {@link #registration(Role, MemberId)} and {@link #lease(Role)} build a key that keeps
 * this rule.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * CoordinationKey mine = CoordinationKey.registration(Role.of("controller"), me);
 * CoordinationKey theirs = CoordinationKey.lease(Role.of("controller"));
 * byte[] bytes = CoordinationCodec.encodeKey(theirs);
 * }</pre>
 *
 * @param kind the record the key names
 * @param role the role the record belongs to
 * @param member the member for a registration key, and empty for a lease key
 */
public record CoordinationKey(RecordKind kind, Role role, Optional<MemberId> member) {
    /**
     * Rejects a null component and a key whose member does not match its kind.
     *
     * @param kind the record the key names
     * @param role the role the record belongs to
     * @param member the member for a registration key, and empty for a lease key
     */
    public CoordinationKey {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(member, "member");
        if (kind == RecordKind.REGISTRATION && member.isEmpty()) {
            throw new CoordinationException("a registration key names one member");
        }
        if (kind == RecordKind.LEASE && member.isPresent()) {
            throw new CoordinationException(
                    "a lease key names no member, got " + member.orElseThrow());
        }
    }

    /**
     * Builds the key of the registration of one member of a role.
     *
     * @param role the role the member competes for
     * @param member the member that registers
     * @return the registration key of that member
     */
    public static CoordinationKey registration(Role role, MemberId member) {
        return new CoordinationKey(RecordKind.REGISTRATION, role,
                Optional.of(Objects.requireNonNull(member, "member")));
    }

    /**
     * Builds the key of the lease of a role.
     *
     * @param role the role the lease belongs to
     * @return the lease key of that role, which carries no member
     */
    public static CoordinationKey lease(Role role) {
        return new CoordinationKey(RecordKind.LEASE, role, Optional.empty());
    }
}
