package io.krabka.streams.coordination;

import java.time.Duration;
import java.util.Objects;

/**
 * The lease clock of one lease record under one policy.
 *
 * <p>Every method takes and returns milliseconds since the Unix epoch, because a
 * deadline is a coordinate and not an extent. {@link #remainingAt(long)} is the one
 * exception, and it returns the extent that is left.
 *
 * <p>Read the deadline as a hint and not as a guarantee. A holder past its deadline
 * still owns the newest epoch until a challenger mints a newer one. A challenger that
 * reads a live lease still wins the role at once if it calls {@code InitProducerId}
 * anyway. The lease only stops it from trying.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * LeaseTiming timing = config.timing(lease);
 * if (timing.renewDueAt(clock.nowMillis())) {
 *     leadership.renew();
 * }
 * long myTurn = timing.challengeAtMillis(state.rankOf(me).orElseThrow());
 * }</pre>
 */
public final class LeaseTiming {
    private final Lease lease;
    private final LeaseConfig config;

    /**
     * Binds a lease record to a policy.
     *
     * @param lease the lease record to read
     * @param config the policy that supplies the renew interval and the stagger
     * @throws NullPointerException if the lease or the config is null
     */
    public LeaseTiming(Lease lease, LeaseConfig config) {
        this.lease = Objects.requireNonNull(lease, "lease");
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * Returns the lease record behind this clock.
     *
     * @return the lease this clock reads
     */
    public Lease lease() {
        return lease;
    }

    /**
     * Returns the instant the lease expires.
     *
     * @return the deadline, in milliseconds since the Unix epoch
     */
    public long expiresAtMillis() {
        return lease.deadline();
    }

    /**
     * Reports whether the lease is live at one instant.
     *
     * <p>The lease is live before its deadline and expired from the deadline on. The
     * rank 0 challenger challenges at exactly the deadline, so the two tests leave no
     * gap and no overlap.
     *
     * @param nowMillis the instant to test, in milliseconds since the Unix epoch
     * @return true when the instant falls before the deadline
     */
    public boolean liveAt(long nowMillis) {
        return nowMillis < lease.deadline();
    }

    /**
     * Returns the extent that is left before the deadline.
     *
     * @param nowMillis the instant to measure from, in milliseconds since the Unix
     *     epoch
     * @return the remaining extent, and zero from the deadline on
     */
    public Duration remainingAt(long nowMillis) {
        long left = lease.deadline() - nowMillis;
        return left <= 0 ? Duration.ZERO : Duration.ofMillis(left);
    }

    /**
     * Returns the instant at which the holder writes its next renewal.
     *
     * <p>The value is the grant instant plus the renew interval, and it never passes the
     * deadline. A holder that follows it always writes before it loses the lease.
     *
     * @return the renewal instant, in milliseconds since the Unix epoch
     */
    public long renewAtMillis() {
        long renewAt = Instants.add(lease.grantedAt(), config.renewInterval().toMillis());
        return Math.min(renewAt, lease.deadline());
    }

    /**
     * Reports whether the holder writes a renewal at one instant.
     *
     * @param nowMillis the instant to test, in milliseconds since the Unix epoch
     * @return true when the renewal instant has passed
     */
    public boolean renewDueAt(long nowMillis) {
        return nowMillis >= renewAtMillis();
    }

    /**
     * Returns the instant at which a challenger of one rank challenges.
     *
     * <p>Rank 0 challenges at the deadline, and each later rank adds one challenge
     * stagger. The stagger saves epoch churn and gives no safety.
     * {@code InitProducerId} is atomic at the coordinator, so a simultaneous challenge
     * by every standby still leaves exactly one member with the newest epoch.
     *
     * @param rank the challenge rank, where zero is the first standby
     * @return the challenge instant, in milliseconds since the Unix epoch
     */
    public long challengeAtMillis(int rank) {
        return Instants.add(lease.deadline(), config.challengeDelayMillis(rank));
    }
}
