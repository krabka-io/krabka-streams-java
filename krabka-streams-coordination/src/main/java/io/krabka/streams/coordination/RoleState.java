package io.krabka.streams.coordination;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * The state that a reader folds out of the records of one role.
 *
 * <p>The roster is in offset order, which is registration order. The lease is the last
 * lease record of the role, and it is absent when no member holds the role or when a
 * tombstone cleared it.
 *
 * <p>Build a state with {@link #fromRecords(Role, Iterable)}, or fold records one at a
 * time with {@link RoleStateBuilder}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * RoleState state = RoleState.fromRecords(role, transport.readRoleRecords(role));
 * OptionalInt rank = state.rankOf(me);
 * Optional<MemberId> holder = state.holder();
 * }</pre>
 */
public final class RoleState {
    private static final RoleState EMPTY = new RoleState(List.of(), Optional.empty());

    private final List<RosterEntry> roster;
    private final Optional<Lease> lease;

    RoleState(List<RosterEntry> roster, Optional<Lease> lease) {
        this.roster = List.copyOf(roster);
        this.lease = lease;
    }

    /**
     * Returns the state of a role that has no record.
     *
     * @return a state with an empty roster and no lease
     */
    public static RoleState empty() {
        return EMPTY;
    }

    /**
     * Folds the records of one role into a role state.
     *
     * <p>The caller reads the partition of the role in offset order and passes every
     * record. The fold gives the same answer for another order, because it keeps the
     * record of the highest offset for every key. It drops a record of another role, so
     * a caller folds a partition that holds several roles and needs no filter of its
     * own.
     *
     * @param role the role to collect
     * @param entries the decoded records of the partition
     * @return the folded state of that role
     * @throws NullPointerException if the role or the entries are null
     */
    public static RoleState fromRecords(Role role, Iterable<CoordinationEntry> entries) {
        RoleStateBuilder builder = new RoleStateBuilder(role);
        for (CoordinationEntry entry : Objects.requireNonNull(entries, "entries")) {
            builder.apply(entry);
        }
        return builder.build();
    }

    /**
     * Returns the candidates of the role, in registration order.
     *
     * @return the roster, oldest registration first
     */
    public List<RosterEntry> roster() {
        return roster;
    }

    /**
     * Returns the lease of the role.
     *
     * @return the last lease record, and empty when no member holds the role
     */
    public Optional<Lease> lease() {
        return lease;
    }

    /**
     * Returns the member the lease names.
     *
     * <p>The holder of an expired lease is still the holder. Ask
     * {@link LeaseTiming#liveAt(long)} whether the lease is live.
     *
     * @return the holder, and empty when the role has no lease
     */
    public Optional<MemberId> holder() {
        return lease.map(Lease::member);
    }

    /**
     * Returns the roster entry of one member.
     *
     * @param member the member to look up
     * @return the entry, and empty when the member did not register
     */
    public Optional<RosterEntry> entry(MemberId member) {
        Objects.requireNonNull(member, "member");
        return roster.stream().filter(entry -> entry.member().equals(member)).findFirst();
    }

    /**
     * Returns the challenge rank of one member.
     *
     * <p>The rank is the index of the member in the roster after the removal of the
     * current holder, so the first standby takes rank 0. The holder itself keeps rank 0.
     * A holder reaches the rank test only after its own lease expired, and it still owns
     * the newest epoch at that point, so it reclaims the role for less churn than a
     * failover costs.
     *
     * @param member the member to rank
     * @return the rank, and empty when the member did not register
     */
    public OptionalInt rankOf(MemberId member) {
        if (entry(member).isEmpty()) {
            return OptionalInt.empty();
        }
        Optional<MemberId> holder = holder();
        if (holder.filter(member::equals).isPresent()) {
            return OptionalInt.of(0);
        }
        int rank = 0;
        for (RosterEntry entry : roster) {
            if (holder.filter(entry.member()::equals).isPresent()) {
                continue;
            }
            if (entry.member().equals(member)) {
                return OptionalInt.of(rank);
            }
            rank++;
        }
        return OptionalInt.empty();
    }

    /**
     * Reports whether another object is a state with the same roster and lease.
     *
     * @param other the object to compare against
     * @return true when the other object carries this roster and this lease
     */
    @Override
    public boolean equals(Object other) {
        return other instanceof RoleState state
                && roster.equals(state.roster)
                && lease.equals(state.lease);
    }

    /**
     * Returns a hash of the roster and the lease.
     *
     * @return the hash code of the two components
     */
    @Override
    public int hashCode() {
        return Objects.hash(roster, lease);
    }

    /**
     * Returns the roster and the lease.
     *
     * @return a description of this state
     */
    @Override
    public String toString() {
        return "RoleState[roster=" + roster + ", lease=" + lease + "]";
    }
}
