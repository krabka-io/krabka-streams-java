package io.krabka.streams.coordination;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Elects one leader per role over a {@link CoordinationTransport}.
 *
 * <p>The client joins the three pure parts of this module. It reads the coordination
 * topic through the transport, folds a {@link RoleState}, asks {@link Succession} what
 * this member does now, and carries that answer back to the cluster. It owns no state
 * of its own between calls, so two clients of one process see the same cluster.
 *
 * <p>{@link #tryAcquire(Role, MemberId)} runs one pass and never blocks.
 * {@link #acquire(Role, MemberId, Duration)} repeats that pass until the member wins the
 * role or the deadline passes. A test drives {@code tryAcquire} with
 * {@link ManualClock} and needs no sleep and no broker.
 *
 * <p>The epoch is the safety mechanism, and the broker enforces it. This client decides
 * only when a member calls {@code InitProducerId}. A wrong decision makes a failover
 * early or late. A wrong decision never makes two members authoritative.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try (CoordinationClient client = new CoordinationClient(transport)) {
 *     try (Leadership leadership = client.acquire(role, me, Duration.ofMinutes(1))) {
 *         dispatch(leadership.token());
 *     }
 * }
 * }</pre>
 */
public final class CoordinationClient implements AutoCloseable {
    /** The gap between two passes of a blocking acquire, when no decision names one. */
    public static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofMillis(500);

    private final CoordinationTransport transport;
    private final LeaseConfig config;
    private final Clock clock;
    private final Duration retryInterval;

    /**
     * Creates a client with the default lease timings and the clock of the host.
     *
     * @param transport the seam to the cluster
     * @throws NullPointerException if the transport is null
     */
    public CoordinationClient(CoordinationTransport transport) {
        this(transport, LeaseConfig.defaults(), Clock.system(), DEFAULT_RETRY_INTERVAL);
    }

    /**
     * Creates a client.
     *
     * @param transport the seam to the cluster
     * @param config the lease timings of every role this client drives
     * @param clock the source of the current time
     * @param retryInterval the gap between two passes of a blocking acquire, when no
     *     decision names one
     * @throws NullPointerException if an argument is null
     */
    public CoordinationClient(CoordinationTransport transport, LeaseConfig config, Clock clock,
            Duration retryInterval) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.retryInterval = Objects.requireNonNull(retryInterval, "retryInterval");
    }

    /**
     * Returns the lease timings this client applies.
     *
     * @return the lease policy of every role this client drives
     */
    public LeaseConfig config() {
        return config;
    }

    /**
     * Reads and folds the coordination topic for one role.
     *
     * @param role the role to read
     * @return the roster and the last lease record of that role
     * @throws CoordinationException if the read fails or a record does not decode
     */
    public RoleState readState(Role role) {
        return RoleState.fromRecords(role, transport.readRoleRecords(role));
    }

    /**
     * Reports what one role looks like to a third party.
     *
     * <p>The call asks the transaction coordinator for the token of the role and folds
     * the coordination topic. It takes no epoch and it joins no group, so any process
     * calls it.
     *
     * @param role the role to describe
     * @return the token, the roster, and the last lease record of that role
     * @throws CoordinationException if either read fails
     */
    public LeadershipStatus describe(Role role) {
        FencingToken token = transport.describe(role).orElse(FencingToken.NO_EPOCH);
        return new LeadershipStatus(role, token, readState(role));
    }

    /**
     * Runs one election pass for one member.
     *
     * <p>The pass registers the member when the roster does not name it, and it reads
     * the topic again after that append. The pass then asks {@link Succession} what the
     * member does now. It mints an epoch and writes a lease for
     * {@link Decision.Action#CHALLENGE} and for {@link Decision.Action#HOLD}, and it
     * returns an empty result for {@link Decision.Action#WAIT}.
     *
     * <p>A member that a live lease already names still mints a fresh epoch here. The
     * member holds no producer bound to the older epoch in this process, and
     * {@code InitProducerId} fences the incarnation that did.
     *
     * <p>The pass returns an empty result when another member wins the race between the
     * epoch call and the lease write. The broker reports that race as a fence, and the
     * loser reads the newer state on its next pass.
     *
     * @param role the role to compete for
     * @param member the member that competes
     * @return the leadership, and empty when this member does not take the role now
     * @throws CoordinationException if a cluster call fails
     */
    public Optional<Leadership> tryAcquire(Role role, MemberId member) {
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(member, "member");
        RoleState state = readState(role);
        Decision decision = Succession.evaluate(state, member, clock.nowMillis(), config);
        if (decision.action() == Decision.Action.NOT_REGISTERED) {
            transport.register(role, member, clock.nowMillis());
            state = readState(role);
            decision = Succession.evaluate(state, member, clock.nowMillis(), config);
        }
        if (decision.action() == Decision.Action.WAIT
                || decision.action() == Decision.Action.NOT_REGISTERED) {
            return Optional.empty();
        }
        FencingToken token = transport.acquireEpoch(role);
        Lease lease = config.grant(member, token, clock.nowMillis());
        try {
            transport.writeLease(role, token, lease);
        } catch (FencedException lost) {
            return Optional.empty();
        }
        return Optional.of(new Leadership(transport, config, clock, role, member, token, lease));
    }

    /**
     * Competes for one role until this member wins it or the deadline passes.
     *
     * <p>The method repeats {@link #tryAcquire(Role, MemberId)} and sleeps between two
     * passes. It sleeps until the instant that a {@link Decision.Action#WAIT} names, and
     * for {@link #DEFAULT_RETRY_INTERVAL} at most. A caller that drives its own loop
     * should call {@code tryAcquire} instead.
     *
     * @param role the role to compete for
     * @param member the member that competes
     * @param timeout how long this member waits for the role
     * @return the leadership this member won
     * @throws CoordinationException if the deadline passes before this member wins the
     *     role, if a cluster call fails, or if another thread interrupts this one
     */
    public Leadership acquire(Role role, MemberId member, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long deadline = Instants.add(clock.nowMillis(), timeout.toMillis());
        while (true) {
            Optional<Leadership> won = tryAcquire(role, member);
            if (won.isPresent()) {
                return won.orElseThrow();
            }
            long now = clock.nowMillis();
            if (now >= deadline) {
                throw new CoordinationException(
                        "member " + member + " did not win role " + role + " before the deadline");
            }
            sleep(Math.min(retryInterval.toMillis(), deadline - now));
        }
    }

    /**
     * Closes the transport this client drives.
     */
    @Override
    public void close() {
        transport.close();
    }

    private void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CoordinationException("the wait for a role was interrupted", interrupted);
        }
    }
}
