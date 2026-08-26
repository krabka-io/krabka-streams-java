package io.krabka.streams.coordination;

/**
 * A source of the current time.
 *
 * <p>The succession rules and the lease clock take an instant as a parameter, so they
 * need no clock at all. This interface is the seam for the code around them that reads
 * a real clock. {@link CoordinationClient} and {@link Leadership} read it. A test
 * supplies {@link ManualClock}, drives time by hand, and sleeps for nothing.
 *
 * <p>Every instant this module passes is a coordinate on the epoch-millisecond line.
 * An extent of time is a {@link java.time.Duration}. The two kinds never mix in one
 * value.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * Clock clock = Clock.system();
 * long now = clock.nowMillis();
 * boolean live = config.timing(lease).liveAt(now);
 * }</pre>
 */
@FunctionalInterface
public interface Clock {
    /**
     * Returns the current time.
     *
     * @return the milliseconds since the Unix epoch
     */
    long nowMillis();

    /**
     * Returns the clock of the host.
     *
     * @return a clock that reads {@link System#currentTimeMillis()}
     */
    static Clock system() {
        return System::currentTimeMillis;
    }
}
