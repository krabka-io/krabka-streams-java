package io.krabka.streams.coordination;

import java.util.Objects;

/**
 * One member of the roster of a role.
 *
 * <p>The offset is the offset of the registration record of the member in the partition
 * of the role, and it is the join sequence of that member. Log compaction keeps the
 * offset of every record it retains, so a reader that walks the partition in offset
 * order sees the registrations in registration order.
 *
 * <p>Rank comes from this offset, and not from a configuration file. A recovered node
 * registers again, takes a higher offset, and lands at the tail of the roster. That is
 * the no-failback rule. A rank from a configuration file would put the recovered node
 * at the front, and the node would then preempt the member that replaced it.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * for (RosterEntry entry : state.roster()) {
 *     System.out.println(entry.member() + " joined at offset " + entry.offset());
 * }
 * }</pre>
 *
 * @param member the member that registered
 * @param offset the offset of the registration record of the member
 * @param registeredAt the time the member registered, in milliseconds since the Unix
 *     epoch
 */
public record RosterEntry(MemberId member, long offset, long registeredAt) {
    /**
     * Rejects a null member.
     *
     * @param member the member that registered
     * @param offset the offset of the registration record of the member
     * @param registeredAt the time the member registered, in milliseconds since the
     *     Unix epoch
     */
    public RosterEntry {
        Objects.requireNonNull(member, "member");
    }
}
