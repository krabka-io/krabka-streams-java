package io.krabka.streams.coordination;

import java.util.List;
import java.util.Optional;

/**
 * The seam between the succession rules and the cluster.
 *
 * <p>This interface holds every cluster operation that the coordination rules need, and
 * it holds nothing else. The rules decide who registers, who challenges, and when. This
 * interface carries those decisions to the brokers.
 * {@link KafkaCoordinationTransport} is the implementation that talks to a cluster, and
 * a test supplies its own so the rules run with no broker.
 *
 * <h2>Which calls carry authority</h2>
 *
 * <p>{@link #acquireEpoch(Role)} and {@link #writeLease(Role, FencingToken, Lease)} are
 * the guarded pair. The first mints the epoch of the role and fences the member that
 * held it before. The second writes under that epoch, and the broker rejects the write
 * when a later member has taken the role. A deposed holder learns that it lost the role
 * from {@link FencedException}, and from nothing else.
 *
 * <p>{@link #register(Role, MemberId, long)} carries no authority. A candidate holds no epoch
 * when it announces itself, so the registration is a plain append.
 * {@link #readRoleRecords(Role)} and {@link #describe(Role)} only read.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * transport.register(role, me, clock.nowMillis());
 * RoleState state = RoleState.fromRecords(role, transport.readRoleRecords(role));
 * if (Succession.evaluate(state, me, now, config).action() == Decision.Action.CHALLENGE) {
 *     FencingToken token = transport.acquireEpoch(role);
 *     transport.writeLease(role, token, config.grant(me, token, now));
 * }
 * }</pre>
 */
public interface CoordinationTransport extends AutoCloseable {
    /**
     * Mints a new epoch for a role and fences the member that held it.
     *
     * <p>The implementation calls {@code InitProducerId} with
     * {@code transactional.id = <role>}, and it keeps the producer that the epoch binds,
     * because {@link #writeLease(Role, FencingToken, Lease)} writes through that
     * producer. The transaction coordinator picks the epoch, so the quorum mints the
     * value and the value only grows.
     *
     * @param role the role to take
     * @return the token the transaction coordinator minted
     * @throws CoordinationException if the coordinator refuses the call or the
     *     connection fails
     */
    FencingToken acquireEpoch(Role role);

    /**
     * Reads the whole partition of a role and returns the records in offset order.
     *
     * <p>The read takes committed records only, so an aborted lease write stays
     * invisible. The result holds the records of this role and drops every record of
     * another role that shares the partition.
     *
     * @param role the role to read
     * @return the decoded records, oldest first
     * @throws CoordinationException if a fetch fails or a record does not decode
     */
    List<CoordinationEntry> readRoleRecords(Role role);

    /**
     * Appends the registration of one member to the partition of a role.
     *
     * <p>A candidate holds no epoch, so this append sits outside a transaction and
     * carries no authority. Its offset is the join sequence that the succession rules
     * rank on.
     *
     * <p>The caller passes the instant, so the record carries the clock of the member
     * and this interface needs no clock of its own.
     *
     * @param role the role the member competes for
     * @param member the member that announces itself
     * @param registeredAtMillis the instant of the registration, in milliseconds since
     *     the Unix epoch
     * @throws CoordinationException if the append fails
     */
    void register(Role role, MemberId member, long registeredAtMillis);

    /**
     * Writes the lease of a role in a transaction under one token.
     *
     * <p>The broker rejects the write when a later member has taken the role, and that
     * rejection is how a deposed holder learns it lost the role.
     *
     * @param role the role the lease belongs to
     * @param token the token this transport minted for the role
     * @param lease the lease record to write
     * @throws FencedException if the broker rejects the token
     * @throws CoordinationException if this transport never minted the token, or if the
     *     write fails for another reason
     */
    void writeLease(Role role, FencingToken token, Lease lease);

    /**
     * Clears the lease of a role with a tombstone, in a transaction under one token.
     *
     * <p>A member writes this when it resigns. A standby then challenges at once rather
     * than at the deadline of the record the member left behind.
     *
     * @param role the role to release
     * @param token the token this transport minted for the role
     * @throws FencedException if the broker rejects the token
     * @throws CoordinationException if this transport never minted the token, or if the
     *     write fails for another reason
     */
    void clearLease(Role role, FencingToken token);

    /**
     * Asks the transaction coordinator which token holds a role now.
     *
     * <p>A third party calls this to check the authority of a writer. The call joins no
     * group and it takes no epoch.
     *
     * @param role the role to look up
     * @return the current token, and empty when no member has ever taken the role
     * @throws CoordinationException if the coordinator lookup fails
     */
    Optional<FencingToken> describe(Role role);

    /**
     * Releases the resources this transport created.
     *
     * <p>A transport closes the producers it built for itself. It does not close a
     * client that the caller passed in.
     */
    @Override
    void close();
}
