package io.krabka.streams.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class LeaseTimingTest {
    private static final long GRANTED_AT = 1_700_000_000_000L;
    private static final long DEADLINE = 1_700_000_030_000L;
    private static final LeaseConfig CONFIG = LeaseConfig.defaults();
    private static final Lease LEASE = new Lease(
            MemberId.of("node-1"), FencingToken.of(4242L, (short) 7), GRANTED_AT, DEADLINE);

    @Test
    void countsTheLeaseLiveBeforeItsDeadlineAndExpiredFromTheDeadlineOn() {
        LeaseTiming timing = CONFIG.timing(LEASE);

        assertThat(timing.liveAt(DEADLINE - 1)).isTrue();
        assertThat(timing.liveAt(DEADLINE)).isFalse();
        assertThat(timing.expiresAtMillis()).isEqualTo(DEADLINE);
    }

    @Test
    void leavesNoGapBetweenTheLastLiveInstantAndTheFirstChallenge() {
        LeaseTiming timing = CONFIG.timing(LEASE);

        assertThat(timing.liveAt(timing.challengeAtMillis(0) - 1)).isTrue();
        assertThat(timing.liveAt(timing.challengeAtMillis(0))).isFalse();
    }

    @Test
    void reportsTheRemainingExtentAndZeroAfterTheDeadline() {
        LeaseTiming timing = CONFIG.timing(LEASE);

        assertThat(timing.remainingAt(GRANTED_AT)).isEqualTo(Duration.ofSeconds(30));
        assertThat(timing.remainingAt(DEADLINE)).isEqualTo(Duration.ZERO);
        assertThat(timing.remainingAt(DEADLINE + 10_000)).isEqualTo(Duration.ZERO);
    }

    @Test
    void renewsOneIntervalAfterTheGrant() {
        LeaseTiming timing = CONFIG.timing(LEASE);

        assertThat(timing.renewAtMillis()).isEqualTo(GRANTED_AT + 10_000);
        assertThat(timing.renewDueAt(GRANTED_AT + 9_999)).isFalse();
        assertThat(timing.renewDueAt(GRANTED_AT + 10_000)).isTrue();
    }

    @Test
    void neverRenewsPastTheDeadline() {
        // A renew interval close to the duration still leaves the holder one write before it loses
        // the lease, because the renewal instant is capped at the deadline.
        LeaseConfig tight =
                LeaseConfig.of(Duration.ofSeconds(30), Duration.ofSeconds(29), Duration.ofSeconds(5));
        Lease shortLease = new Lease(
                MemberId.of("node-1"), FencingToken.of(1L, (short) 1), GRANTED_AT, GRANTED_AT + 10_000);

        assertThat(tight.timing(shortLease).renewAtMillis()).isEqualTo(GRANTED_AT + 10_000);
    }

    @Test
    void staggersEachRankOneIntervalPastTheDeadline() {
        LeaseTiming timing = CONFIG.timing(LEASE);

        assertThat(timing.challengeAtMillis(0)).isEqualTo(DEADLINE);
        assertThat(timing.challengeAtMillis(1)).isEqualTo(DEADLINE + 5_000);
        assertThat(timing.challengeAtMillis(4)).isEqualTo(DEADLINE + 20_000);
    }

    @Test
    void saturatesAChallengeInstantAtTheEndOfTheInstantLine() {
        Lease late = new Lease(
                MemberId.of("node-1"), FencingToken.of(1L, (short) 1), 0L, Long.MAX_VALUE - 5);

        assertThat(CONFIG.timing(late).challengeAtMillis(1)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void carriesTheLeaseItReads() {
        assertThat(new LeaseTiming(LEASE, CONFIG).lease()).usingRecursiveComparison().isEqualTo(LEASE);
    }
}
