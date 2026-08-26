package io.krabka.streams.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ManualClockTest {
    @Test
    void readsTheInstantItWasSetTo() {
        ManualClock clock = new ManualClock(1_700_000_000_000L);

        assertThat(clock.nowMillis()).isEqualTo(1_700_000_000_000L);
        clock.set(5L);
        assertThat(clock.nowMillis()).isEqualTo(5L);
    }

    @Test
    void movesForwardAndBackwards() {
        ManualClock clock = new ManualClock(1_000L);

        clock.advance(Duration.ofSeconds(30));
        assertThat(clock.nowMillis()).isEqualTo(31_000L);
        clock.advance(Duration.ofSeconds(-1));
        assertThat(clock.nowMillis()).isEqualTo(30_000L);
    }

    @Test
    void saturatesAtTheEndOfTheInstantLine() {
        ManualClock clock = new ManualClock(Long.MAX_VALUE - 5);

        clock.advance(Duration.ofDays(1));
        assertThat(clock.nowMillis()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void readsTheHostClockThroughTheSystemFactory() {
        long before = System.currentTimeMillis();

        assertThat(Clock.system().nowMillis()).isGreaterThanOrEqualTo(before);
    }
}
