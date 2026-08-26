package io.krabka.streams.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class LeaseConfigTest {
    private static final MemberId MEMBER = MemberId.of("node-1");
    private static final FencingToken TOKEN = FencingToken.of(4242L, (short) 7);

    @Test
    void takesThirtyTenAndFiveSecondsByDefault() {
        assertThat(LeaseConfig.defaults())
                .usingRecursiveComparison()
                .isEqualTo(LeaseConfig.of(
                        Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(5)));
        assertThat(LeaseConfig.defaults().renewsWithMargin()).isTrue();
    }

    @Test
    void rejectsAnExtentThatIsNotPositive() {
        assertThatThrownBy(() -> LeaseConfig.of(Duration.ZERO, Duration.ofSeconds(1), Duration.ofSeconds(1)))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("the lease duration must be a positive extent, got 0 ms");
        assertThatThrownBy(() ->
                        LeaseConfig.of(Duration.ofSeconds(30), Duration.ofSeconds(-1), Duration.ofSeconds(1)))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("the renew interval must be a positive extent, got -1000 ms");
        assertThatThrownBy(() -> LeaseConfig.of(Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ZERO))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("the challenge stagger must be a positive extent, got 0 ms");
    }

    @Test
    void rejectsARenewIntervalThatIsNotShorterThanTheLease() {
        assertThatThrownBy(() ->
                        LeaseConfig.of(Duration.ofSeconds(30), Duration.ofSeconds(30), Duration.ofSeconds(5)))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("the renew interval of 30000 ms is not shorter than the lease duration of 30000 ms");
    }

    @Test
    void acceptsARenewIntervalOutsideTheRecommendedThird() {
        LeaseConfig tight =
                LeaseConfig.of(Duration.ofSeconds(30), Duration.ofSeconds(20), Duration.ofSeconds(5));

        assertThat(tight.renewsWithMargin()).isFalse();
        assertThat(tight.renewInterval()).isEqualTo(Duration.ofSeconds(20));
    }

    @Test
    void staggersOneChallengeDelayPerRank() {
        LeaseConfig config = LeaseConfig.defaults();

        assertThat(config.challengeDelayMillis(0)).isZero();
        assertThat(config.challengeDelayMillis(1)).isEqualTo(5_000L);
        assertThat(config.challengeDelayMillis(3)).isEqualTo(15_000L);
    }

    @Test
    void saturatesAChallengeDelayThatWouldWrap() {
        LeaseConfig wide = LeaseConfig.of(
                Duration.ofMillis(Long.MAX_VALUE),
                Duration.ofSeconds(10),
                Duration.ofMillis(Long.MAX_VALUE / 2));

        assertThat(wide.challengeDelayMillis(3)).isEqualTo(Long.MAX_VALUE);
        assertThat(LeaseConfig.defaults().challengeDelayMillis(Integer.MAX_VALUE))
                .isEqualTo(5_000L * Integer.MAX_VALUE);
        assertThat(LeaseConfig.defaults().challengeDelayMillis(-1)).isZero();
    }

    @Test
    void grantsALeaseThatEndsOneDurationLater() {
        assertThat(LeaseConfig.defaults().grant(MEMBER, TOKEN, 1_700_000_000_000L))
                .usingRecursiveComparison()
                .isEqualTo(new Lease(MEMBER, TOKEN, 1_700_000_000_000L, 1_700_000_030_000L));
    }

    @Test
    void saturatesAGrantAtTheEndOfTheInstantLine() {
        assertThat(LeaseConfig.defaults().grant(MEMBER, TOKEN, Long.MAX_VALUE - 5).deadline())
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void treatsTwoConfigsOfOneSetOfExtentsAsEqual() {
        assertThat(LeaseConfig.defaults())
                .isEqualTo(LeaseConfig.of(
                        Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .hasSameHashCodeAs(LeaseConfig.of(
                        Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .isNotEqualTo(LeaseConfig.of(
                        Duration.ofSeconds(60), Duration.ofSeconds(10), Duration.ofSeconds(5)));
        assertThat(LeaseConfig.defaults())
                .hasToString("LeaseConfig[duration=30000ms, renewInterval=10000ms, challengeStagger=5000ms]");
    }
}
