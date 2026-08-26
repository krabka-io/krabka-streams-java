package io.krabka.streams.coordination;

import java.util.Objects;

/**
 * The member that holds a role now, and the time its claim ends.
 *
 * <p>The token is the authority. The broker fences a write that carries an older token,
 * and no clock takes part in that check. The deadline is an anti-flap device. A standby
 * waits for the deadline to pass before it challenges the holder, so a short pause does
 * not move the role. A wrong deadline makes a failover early or late. A wrong deadline
 * never makes two members authoritative.
 *
 * <p>The holder writes the record inside a transaction under the epoch of the role, so
 * a lease record that reached the log is proof that its author held the epoch. A reader
 * uses {@code read_committed}, so an aborted lease write stays invisible.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * Lease lease = config.grant(me, token, clock.nowMillis());
 * transport.writeLease(role, token, lease);
 * boolean live = config.timing(lease).liveAt(clock.nowMillis());
 * }</pre>
 *
 * @param member the member that holds the role
 * @param token the quorum-minted proof of the claim of the holder
 * @param grantedAt the time the holder took the lease, in milliseconds since the Unix
 *     epoch; a renewal moves it forward
 * @param deadline the time the lease expires, in milliseconds since the Unix epoch
 */
public record Lease(MemberId member, FencingToken token, long grantedAt, long deadline)
        implements CoordinationValue {
    /**
     * Rejects a null member and a null token.
     *
     * @param member the member that holds the role
     * @param token the quorum-minted proof of the claim of the holder
     * @param grantedAt the time the holder took the lease, in milliseconds since the
     *     Unix epoch
     * @param deadline the time the lease expires, in milliseconds since the Unix epoch
     */
    public Lease {
        Objects.requireNonNull(member, "member");
        Objects.requireNonNull(token, "token");
    }

    /**
     * Returns the kind of the key that a lease sits under.
     *
     * @return {@link RecordKind#LEASE}
     */
    @Override
    public RecordKind kind() {
        return RecordKind.LEASE;
    }
}
