package io.krabka.streams.coordination;

import java.time.Duration;
import java.util.Objects;

/**
 * The lease timings of one role.
 *
 * <p>The lease adds no safety. The leadership epoch is the safety mechanism, and the
 * broker enforces it. The lease decides when a standby stops waiting for a quiet
 * holder, and nothing else. A wrong timing makes a failover early or late. A wrong
 * timing never makes two members authoritative.
 *
 * <p>The three extents are independent of the record layout, so one process holds
 * several roles with different timings. Build a value with
 * {@link #of(Duration, Duration, Duration)}, which rejects a set of extents that cannot
 * work, or take {@link #defaults()}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * LeaseConfig config = LeaseConfig.of(
 *     Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(5));
 * Lease lease = config.grant(me, token, clock.nowMillis());
 * long renewAt = config.timing(lease).renewAtMillis();
 * }</pre>
 */
public final class LeaseConfig {
    /** The default extent of a lease. */
    public static final Duration DEFAULT_LEASE_DURATION = Duration.ofSeconds(30);

    /** The default gap between two renewals by the holder. */
    public static final Duration DEFAULT_RENEW_INTERVAL = Duration.ofSeconds(10);

    /** The default extra delay that one rank of succession adds. */
    public static final Duration DEFAULT_CHALLENGE_STAGGER = Duration.ofSeconds(5);

    /**
     * The part of the lease duration that this module recommends for the renew
     * interval.
     */
    private static final int RECOMMENDED_RENEW_FRACTION = 3;

    private static final LeaseConfig DEFAULTS = new LeaseConfig(
            DEFAULT_LEASE_DURATION, DEFAULT_RENEW_INTERVAL, DEFAULT_CHALLENGE_STAGGER);

    private final Duration duration;
    private final Duration renewInterval;
    private final Duration challengeStagger;

    private LeaseConfig(Duration duration, Duration renewInterval, Duration challengeStagger) {
        this.duration = duration;
        this.renewInterval = renewInterval;
        this.challengeStagger = challengeStagger;
    }

    /**
     * Returns the config of {@link #DEFAULT_LEASE_DURATION},
     * {@link #DEFAULT_RENEW_INTERVAL}, and {@link #DEFAULT_CHALLENGE_STAGGER}.
     *
     * @return the default lease timings
     */
    public static LeaseConfig defaults() {
        return DEFAULTS;
    }

    /**
     * Builds a lease policy from three extents.
     *
     * <p>A caller should set the renew interval to at most a third of the duration. The
     * holder then keeps two more attempts before the deadline. This factory accepts a
     * larger value. Ask {@link #renewsWithMargin()} which side of that bound a value is
     * on.
     *
     * @param duration the extent of a lease that a member takes now
     * @param renewInterval the gap between two renewals by the holder
     * @param challengeStagger the extra delay that one rank of succession adds
     * @return the lease policy of the three extents
     * @throws CoordinationException if an extent is zero or negative, or if the renew
     *     interval is not shorter than the duration
     * @throws NullPointerException if an extent is null
     */
    public static LeaseConfig of(
            Duration duration, Duration renewInterval, Duration challengeStagger) {
        check("lease duration", duration);
        check("renew interval", renewInterval);
        check("challenge stagger", challengeStagger);
        if (renewInterval.compareTo(duration) >= 0) {
            throw new CoordinationException("the renew interval of " + renewInterval.toMillis()
                    + " ms is not shorter than the lease duration of " + duration.toMillis()
                    + " ms");
        }
        return new LeaseConfig(duration, renewInterval, challengeStagger);
    }

    private static void check(String field, Duration value) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative()) {
            throw new CoordinationException(
                    "the " + field + " must be a positive extent, got " + value.toMillis() + " ms");
        }
    }

    /**
     * Returns the extent of a lease that a member takes now.
     *
     * @return the lease duration
     */
    public Duration duration() {
        return duration;
    }

    /**
     * Returns the gap between two renewals by the holder.
     *
     * @return the renew interval
     */
    public Duration renewInterval() {
        return renewInterval;
    }

    /**
     * Returns the extra delay that one rank of succession adds.
     *
     * @return the challenge stagger
     */
    public Duration challengeStagger() {
        return challengeStagger;
    }

    /**
     * Reports whether the renew interval leaves the holder two spare attempts.
     *
     * <p>The interval this module recommends is at most a third of the lease duration. A
     * config outside that bound still works, and the holder then loses the role after
     * fewer missed renewals.
     *
     * @return true when three renew intervals fit inside the lease duration
     */
    public boolean renewsWithMargin() {
        return Instants.multiply(renewInterval.toMillis(), RECOMMENDED_RENEW_FRACTION)
                <= duration.toMillis();
    }

    /**
     * Returns the extra delay that a challenger of one rank takes.
     *
     * <p>Rank 0 takes none. The value saturates, so a very large rank does not wrap the
     * instant that a caller adds it to.
     *
     * @param rank the challenge rank, where zero is the first standby
     * @return the delay in milliseconds
     */
    public long challengeDelayMillis(int rank) {
        return Instants.multiply(challengeStagger.toMillis(), rank);
    }

    /**
     * Builds the lease record that a member writes after it mints a token.
     *
     * <p>A renewal writes the same record with the same token and a later instant, so
     * this one method covers a first grant and a renewal.
     *
     * @param member the member that holds the role
     * @param token the token the transaction coordinator minted for the member
     * @param nowMillis the instant of the grant, in milliseconds since the Unix epoch
     * @return the lease record to write
     * @throws NullPointerException if the member or the token is null
     */
    public Lease grant(MemberId member, FencingToken token, long nowMillis) {
        return new Lease(member, token, nowMillis, Instants.add(nowMillis, duration.toMillis()));
    }

    /**
     * Binds a lease record to this policy and answers the clock questions about it.
     *
     * @param lease the lease record to read
     * @return the lease clock of that record under this policy
     * @throws NullPointerException if the lease is null
     */
    public LeaseTiming timing(Lease lease) {
        return new LeaseTiming(lease, this);
    }

    /**
     * Reports whether another object is a config with the same three extents.
     *
     * @param other the object to compare against
     * @return true when the other object carries these extents
     */
    @Override
    public boolean equals(Object other) {
        return other instanceof LeaseConfig config
                && duration.equals(config.duration)
                && renewInterval.equals(config.renewInterval)
                && challengeStagger.equals(config.challengeStagger);
    }

    /**
     * Returns a hash of the three extents.
     *
     * @return the hash code of the duration, the renew interval, and the stagger
     */
    @Override
    public int hashCode() {
        return Objects.hash(duration, renewInterval, challengeStagger);
    }

    /**
     * Returns the three extents in milliseconds.
     *
     * @return a description of this policy
     */
    @Override
    public String toString() {
        return "LeaseConfig[duration=" + duration.toMillis() + "ms, renewInterval="
                + renewInterval.toMillis() + "ms, challengeStagger="
                + challengeStagger.toMillis() + "ms]";
    }
}
