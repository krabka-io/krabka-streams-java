package io.krabka.streams.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CoordinationClientTest {
    private static final Role ROLE = Role.of("controller");
    private static final MemberId NODE_A = MemberId.of("node-a");
    private static final MemberId NODE_B = MemberId.of("node-b");
    private static final LeaseConfig CONFIG =
            LeaseConfig.of(Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(5));
    private static final long START = 1_700_000_000_000L;

    @Test
    void registersOnTheFirstPassAndTakesTheRoleInTheSamePass() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        CoordinationClient client = client(cluster, clock);

        Optional<Leadership> won = client.tryAcquire(ROLE, NODE_A);

        assertThat(won).isPresent();
        Leadership leadership = won.orElseThrow();
        assertThat(leadership.role()).isEqualTo(ROLE);
        assertThat(leadership.member()).isEqualTo(NODE_A);
        assertThat(leadership.token()).isEqualTo(FencingToken.of(100L, (short) 1));
        assertThat(leadership.lease())
                .usingRecursiveComparison()
                .isEqualTo(new Lease(NODE_A, leadership.token(), START, START + 30_000));
        assertThat(client.readState(ROLE).holder()).contains(NODE_A);
    }

    @Test
    void makesTheSecondCandidateWaitWhileTheLeaseOfTheFirstIsLive() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        CoordinationClient client = client(cluster, clock);
        assertThat(client.tryAcquire(ROLE, NODE_A)).isPresent();

        assertThat(client.tryAcquire(ROLE, NODE_B)).isEmpty();
        clock.set(START + 29_999);
        assertThat(client.tryAcquire(ROLE, NODE_B)).isEmpty();
    }

    @Test
    void letsTheStandbyTakeTheRoleAtTheDeadlineAndFencesTheOldHolder() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        CoordinationClient client = client(cluster, clock);
        Leadership first = client.tryAcquire(ROLE, NODE_A).orElseThrow();
        client.tryAcquire(ROLE, NODE_B);

        clock.set(START + 30_000);
        Leadership second = client.tryAcquire(ROLE, NODE_B).orElseThrow();

        assertThat(second.member()).isEqualTo(NODE_B);
        assertThat(second.token().supersedes(first.token())).isTrue();
        assertThatThrownBy(first::renew).isInstanceOf(FencedException.class);
        assertThat(first.held()).isFalse();
    }

    @Test
    void keepsAtMostOneMemberWithAnUnfencedEpoch() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        CoordinationClient client = client(cluster, clock);
        Leadership first = client.tryAcquire(ROLE, NODE_A).orElseThrow();
        client.tryAcquire(ROLE, NODE_B);
        clock.set(START + 30_000);
        Leadership second = client.tryAcquire(ROLE, NODE_B).orElseThrow();

        assertThat(client.describe(ROLE).token()).isEqualTo(second.token());
        assertThat(client.describe(ROLE).current()).isTrue();
        assertThatThrownBy(() -> first.renew()).isInstanceOf(FencedException.class);
        assertThat(second.renew()).isNotNull();
    }

    @Test
    void reclaimsItsOwnRoleAtItsOwnDeadlineWithNoStandby() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        CoordinationClient client = client(cluster, clock);
        Leadership first = client.tryAcquire(ROLE, NODE_A).orElseThrow();

        clock.set(START + 30_000);
        Leadership again = client.tryAcquire(ROLE, NODE_A).orElseThrow();

        assertThat(again.token().supersedes(first.token())).isTrue();
    }

    @Test
    void takesTheRoleAtOnceAfterAResignation() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        CoordinationClient client = client(cluster, clock);
        Leadership first = client.tryAcquire(ROLE, NODE_A).orElseThrow();
        client.tryAcquire(ROLE, NODE_B);

        first.resign();

        // The resignation removes the deadline anchor, so the standby challenges one stagger past
        // its own registration instead of 30 seconds past a deadline that no longer applies.
        assertThat(client.readState(ROLE).lease()).isEmpty();
        assertThat(client.tryAcquire(ROLE, NODE_B)).isEmpty();
        clock.set(START + 5_000);
        assertThat(client.tryAcquire(ROLE, NODE_B)).isPresent();
    }

    @Test
    void describesARoleThatNoMemberHasTaken() {
        CoordinationClient client = client(new InMemoryCluster(), new ManualClock(START));

        LeadershipStatus status = client.describe(ROLE);

        assertThat(status.token()).isEqualTo(FencingToken.NO_EPOCH);
        assertThat(status.held()).isFalse();
        assertThat(status.current()).isFalse();
        assertThat(status.holder()).isEmpty();
        assertThat(status.lease()).isEmpty();
        assertThat(status.state().roster()).isEmpty();
    }

    @Test
    void reportsAStaleLeaseAsNotCurrent() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        CoordinationClient client = client(cluster, clock);
        assertThat(client.tryAcquire(ROLE, NODE_A)).isPresent();

        // A challenger mints a newer epoch and has not written its own lease yet.
        cluster.acquireEpoch(ROLE);

        LeadershipStatus status = client.describe(ROLE);
        assertThat(status.holder()).contains(NODE_A);
        assertThat(status.current()).isFalse();
        assertThat(status.held()).isTrue();
    }

    @Test
    void returnsAnEmptyPassWhenAnotherMemberWinsTheRace() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        CoordinationClient racing = new CoordinationClient(
                new RacingCluster(cluster), CONFIG, clock, Duration.ofMillis(1));

        assertThat(racing.tryAcquire(ROLE, NODE_A)).isEmpty();
    }

    @Test
    void blocksUntilItWinsTheRole() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        CoordinationClient client = client(cluster, clock);

        Leadership leadership = client.acquire(ROLE, NODE_A, Duration.ofSeconds(1));

        assertThat(leadership.member()).isEqualTo(NODE_A);
    }

    @Test
    void reportsADeadlineThatPassesBeforeThisMemberWinsTheRole() {
        InMemoryCluster cluster = new InMemoryCluster();
        ManualClock clock = new ManualClock(START);
        CoordinationClient client = client(cluster, clock);
        assertThat(client.tryAcquire(ROLE, NODE_A)).isPresent();

        assertThatThrownBy(() -> client.acquire(ROLE, NODE_B, Duration.ZERO))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("member node-b did not win role controller before the deadline");
    }

    @Test
    void closesTheTransportItDrives() {
        InMemoryCluster cluster = new InMemoryCluster();
        try (CoordinationClient client = client(cluster, new ManualClock(START))) {
            assertThat(client.config()).isEqualTo(CONFIG);
        }

        assertThat(cluster.closed()).isTrue();
    }

    private static CoordinationClient client(InMemoryCluster cluster, ManualClock clock) {
        return new CoordinationClient(cluster, CONFIG, clock, Duration.ofMillis(1));
    }

    /** A cluster where another member always mints a newer epoch before the lease write lands. */
    private record RacingCluster(InMemoryCluster cluster) implements CoordinationTransport {
        @Override
        public FencingToken acquireEpoch(Role role) {
            FencingToken mine = cluster.acquireEpoch(role);
            cluster.acquireEpoch(role);
            return mine;
        }

        @Override
        public java.util.List<CoordinationEntry> readRoleRecords(Role role) {
            return cluster.readRoleRecords(role);
        }

        @Override
        public void register(Role role, MemberId member, long registeredAtMillis) {
            cluster.register(role, member, registeredAtMillis);
        }

        @Override
        public void writeLease(Role role, FencingToken token, Lease lease) {
            cluster.writeLease(role, token, lease);
        }

        @Override
        public void clearLease(Role role, FencingToken token) {
            cluster.clearLease(role, token);
        }

        @Override
        public Optional<FencingToken> describe(Role role) {
            return cluster.describe(role);
        }

        @Override
        public void close() {
            cluster.close();
        }
    }
}
