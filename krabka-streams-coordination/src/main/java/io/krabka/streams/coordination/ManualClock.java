package io.krabka.streams.coordination;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A clock that a caller moves by hand.
 *
 * <p>The lease rules take an instant as a parameter, so a test drives a whole failover
 * with no sleep. The clock is safe for concurrent use, so a test shares one clock
 * between a holder and its challengers and moves them all together.
 *
 * <p>{@link #advance(Duration)} saturates at {@link Long#MAX_VALUE}, so a large step
 * does not wrap the instant it produces.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * ManualClock clock = new ManualClock(1_700_000_000_000L);
 * assertThat(Succession.evaluate(state, standby, clock.nowMillis(), config).action())
 *     .isEqualTo(Decision.Action.WAIT);
 * clock.advance(Duration.ofSeconds(30));
 * assertThat(Succession.evaluate(state, standby, clock.nowMillis(), config).action())
 *     .isEqualTo(Decision.Action.CHALLENGE);
 * }</pre>
 */
public final class ManualClock implements Clock {
    private final AtomicLong nowMillis;

    /**
     * Creates a clock that reads one instant.
     *
     * @param nowMillis the instant the clock starts at, in milliseconds since the Unix
     *     epoch
     */
    public ManualClock(long nowMillis) {
        this.nowMillis = new AtomicLong(nowMillis);
    }

    /**
     * Returns the instant the clock reads now.
     *
     * @return the milliseconds since the Unix epoch
     */
    @Override
    public long nowMillis() {
        return nowMillis.get();
    }

    /**
     * Moves the clock to one instant.
     *
     * <p>The clock moves backwards too, so a test reproduces a clock step.
     *
     * @param instantMillis the instant to read from now on, in milliseconds since the
     *     Unix epoch
     */
    public void set(long instantMillis) {
        nowMillis.set(instantMillis);
    }

    /**
     * Moves the clock forward by one extent.
     *
     * @param step the extent to add
     * @throws NullPointerException if the step is null
     */
    public void advance(Duration step) {
        nowMillis.updateAndGet(now -> Instants.add(now, step.toMillis()));
    }
}
