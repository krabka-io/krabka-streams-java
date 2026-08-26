package io.krabka.streams.coordination;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Folds the records of one role into a {@link RoleState}.
 *
 * <p>The builder keeps the record of the highest offset for every key, which is what log
 * compaction keeps. A later registration for a member replaces the earlier one and
 * moves the member to the tail of the roster. A registration tombstone removes the
 * member. A lease tombstone clears the lease.
 *
 * <p>The builder drops a record of another role, so a caller folds a partition that
 * holds several roles and needs no filter of its own.
 *
 * <p>The key carries the identity, because the key is the compaction key. The builder
 * takes the member and the record kind from the key, and it takes neither from the
 * value.
 *
 * <p>One builder is not safe for concurrent use. Confine it to one thread.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * RoleStateBuilder builder = new RoleStateBuilder(Role.of("controller"));
 * for (CoordinationEntry entry : transport.readRoleRecords(builder.role())) {
 *     builder.apply(entry);
 * }
 * RoleState state = builder.build();
 * }</pre>
 */
public final class RoleStateBuilder {
    /** The slot that one member keeps in a fold. */
    private record MemberSlot(long offset, long registeredAt, boolean registered) {
    }

    /** The slot that the lease keeps in a fold. */
    private record LeaseSlot(long offset, Optional<Lease> lease) {
    }

    private final Role role;
    private final Map<MemberId, MemberSlot> members = new HashMap<>();
    private LeaseSlot lease;

    /**
     * Creates an empty fold for one role.
     *
     * @param role the role to collect
     * @throws NullPointerException if the role is null
     */
    public RoleStateBuilder(Role role) {
        this.role = Objects.requireNonNull(role, "role");
    }

    /**
     * Returns the role that this fold collects.
     *
     * @return the role the builder was created for
     */
    public Role role() {
        return role;
    }

    /**
     * Applies one record of the coordination partition.
     *
     * <p>The builder drops a record of another role, and it drops a value whose kind
     * does not match the kind of its key. {@link CoordinationCodec} reads the kind from
     * the key, so a decoder cannot produce such a pair.
     *
     * @param entry the decoded record and the offset it sits at
     * @return this builder, so a caller chains calls
     * @throws NullPointerException if the entry is null
     */
    public RoleStateBuilder apply(CoordinationEntry entry) {
        Objects.requireNonNull(entry, "entry");
        CoordinationKey key = entry.key();
        if (!key.role().equals(role)) {
            return this;
        }
        Optional<CoordinationValue> value = entry.value();
        if (key.kind() == RecordKind.REGISTRATION) {
            MemberId member = key.member().orElseThrow();
            if (value.orElse(null) instanceof Registration registration) {
                putMember(member,
                        new MemberSlot(entry.offset(), registration.registeredAt(), true));
            } else if (value.isEmpty()) {
                putMember(member, new MemberSlot(entry.offset(), 0L, false));
            }
            return this;
        }
        if (value.orElse(null) instanceof Lease held) {
            putLease(entry.offset(), Optional.of(held));
        } else if (value.isEmpty()) {
            putLease(entry.offset(), Optional.empty());
        }
        return this;
    }

    /**
     * Builds the role state, with the roster in offset order.
     *
     * @return the folded state of the role
     */
    public RoleState build() {
        List<RosterEntry> roster = new ArrayList<>(members.size());
        members.forEach((member, slot) -> {
            if (slot.registered()) {
                roster.add(new RosterEntry(member, slot.offset(), slot.registeredAt()));
            }
        });
        roster.sort(Comparator.comparingLong(RosterEntry::offset));
        return new RoleState(roster, lease == null ? Optional.empty() : lease.lease());
    }

    private void putMember(MemberId member, MemberSlot slot) {
        MemberSlot held = members.get(member);
        if (held != null && held.offset() >= slot.offset()) {
            return;
        }
        members.put(member, slot);
    }

    private void putLease(long offset, Optional<Lease> held) {
        if (lease != null && lease.offset() >= offset) {
            return;
        }
        lease = new LeaseSlot(offset, held);
    }
}
