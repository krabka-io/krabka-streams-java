package io.krabka.streams.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class LeadershipTest {
    private static final Role ROLE = Role.of("controller");
    private static final MemberId NODE_A = MemberId.of("node-a");
    private static final MemberId NODE_B = MemberId.of("node-b");
    private static final LeaseConfig CONFIG =
            LeaseConfig.of(Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(5));
    private static final long START = 1_700_000_000_000L;

    @Test
    void renewsWithTheSameTokenAndALaterGrantInstant() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        Leadership leadership = acquire(cluster, clock);

        clock.set(START + 10_000);
        Lease renewed = leadership.renew();

        assertThat(renewed)
                .usingRecursiveComparison()
                .isEqualTo(new Lease(NODE_A, leadership.token(), START + 10_000, START + 40_000));
        assertThat(leadership.lease()).usingRecursiveComparison().isEqualTo(renewed);
    }

    @Test
    void reportsARenewalOneIntervalAfterTheGrant() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        Leadership leadership = acquire(cluster, clock);

        assertThat(leadership.renewDue()).isFalse();
        clock.set(START + 9_999);
        assertThat(leadership.renewDue()).isFalse();
        clock.set(START + 10_000);
        assertThat(leadership.renewDue()).isTrue();
        assertThat(leadership.timing().expiresAtMillis()).isEqualTo(START + 30_000);
    }

    @Test
    void reportsTheLossOfTheRoleOnceAndThenRefusesEveryCall() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        Leadership leadership = acquire(cluster, clock);
        cluster.acquireEpoch(ROLE);

        assertThatThrownBy(leadership::renew)
                .isInstanceOf(FencedException.class)
                .hasMessage("fenced: another member holds role controller");
        assertThat(leadership.held()).isFalse();
        assertThatThrownBy(leadership::renew)
                .isInstanceOf(CoordinationException.class)
                .hasMessage("member node-a no longer holds role controller");
    }

    @Test
    void namesTheRoleItLost() {
        FencedException fenced = new FencedException(ROLE, null);

        assertThat(fenced.role()).isEqualTo(ROLE);
    }

    @Test
    void clearsTheLeaseOnAResignation() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        Leadership leadership = acquire(cluster, clock);

        leadership.resign();

        assertThat(RoleState.fromRecords(ROLE, cluster.readRoleRecords(ROLE)).lease()).isEmpty();
        assertThat(leadership.held()).isFalse();
        assertThatThrownBy(leadership::resign).isInstanceOf(CoordinationException.class);
    }

    @Test
    void resignsOnCloseAndTreatsASecondCloseAsANoOp() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        Leadership leadership = acquire(cluster, clock);

        leadership.close();
        leadership.close();

        assertThat(RoleState.fromRecords(ROLE, cluster.readRoleRecords(ROLE)).lease()).isEmpty();
        assertThat(leadership.held()).isFalse();
    }

    @Test
    void swallowsAFenceOnClose() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        Leadership leadership = acquire(cluster, clock);
        cluster.acquireEpoch(ROLE);

        leadership.close();

        assertThat(leadership.held()).isFalse();
    }

    @Test
    void keepsTheTokenTheCallerPassesToEveryGuardedWrite() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        Leadership first = acquire(cluster, clock);

        clock.set(START + 30_000);
        Leadership second = new CoordinationClient(cluster, CONFIG, clock, Duration.ofMillis(1))
                .tryAcquire(ROLE, NODE_B)
                .orElseThrow();

        assertThat(second.token()).isNotEqualTo(first.token());
        assertThat(second.token().supersedes(first.token())).isTrue();
    }

    private static Leadership acquire(InMemoryCluster cluster, ManualClock clock) {
        return new CoordinationClient(cluster, CONFIG, clock, Duration.ofMillis(1))
                .tryAcquire(ROLE, NODE_A)
                .orElseThrow();
    }
}
