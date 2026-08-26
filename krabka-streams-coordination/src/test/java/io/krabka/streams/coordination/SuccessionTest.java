package io.krabka.streams.coordination;

import static io.krabka.streams.coordination.CoordinationRecords.CONTROLLER;
import static io.krabka.streams.coordination.CoordinationRecords.lease;
import static io.krabka.streams.coordination.CoordinationRecords.leaseTombstone;
import static io.krabka.streams.coordination.CoordinationRecords.member;
import static io.krabka.streams.coordination.CoordinationRecords.registration;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class SuccessionTest {
    private static final LeaseConfig CONFIG =
            LeaseConfig.of(Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(5));
    private static final long JOINED = 1_000L;
    private static final long GRANTED = 2_000L;
    private static final long DEADLINE = 32_000L;

    private static RoleState threeCandidatesWithAHolder() {
        return RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(1, CONTROLLER, "node-a", JOINED),
                        registration(2, CONTROLLER, "node-b", JOINED),
                        registration(3, CONTROLLER, "node-c", JOINED),
                        lease(4, CONTROLLER, "node-a", GRANTED, DEADLINE)));
    }

    @Test
    void tellsAnUnregisteredMemberToRegisterFirst() {
        assertThat(Succession.evaluate(threeCandidatesWithAHolder(), member("node-z"), GRANTED, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.notRegistered());
    }

    @Test
    void tellsTheHolderOfALiveLeaseToHold() {
        assertThat(Succession.evaluate(
                        threeCandidatesWithAHolder(), member("node-a"), DEADLINE - 1, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.hold());
    }

    @Test
    void tellsAStandbyToWaitUntilItsOwnStaggeredInstant() {
        RoleState state = threeCandidatesWithAHolder();

        assertThat(Succession.evaluate(state, member("node-b"), GRANTED, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.waitUntil(DEADLINE));
        assertThat(Succession.evaluate(state, member("node-c"), GRANTED, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.waitUntil(DEADLINE + 5_000));
    }

    @Test
    void tellsTheFirstStandbyToChallengeAtTheDeadline() {
        RoleState state = threeCandidatesWithAHolder();

        assertThat(Succession.evaluate(state, member("node-b"), DEADLINE, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.challenge());
        assertThat(Succession.evaluate(state, member("node-c"), DEADLINE, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.waitUntil(DEADLINE + 5_000));
    }

    @Test
    void tellsTheSecondStandbyToChallengeOneStaggerLater() {
        assertThat(Succession.evaluate(
                        threeCandidatesWithAHolder(), member("node-c"), DEADLINE + 5_000, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.challenge());
    }

    @Test
    void letsTheHolderOfAnExpiredLeaseReclaimItsOwnRoleAtItsOwnDeadline() {
        // The holder keeps rank 0, so it reclaims for less churn than a failover costs.
        assertThat(Succession.evaluate(threeCandidatesWithAHolder(), member("node-a"), DEADLINE, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.challenge());
    }

    @Test
    void anchorsOnTheRegistrationInstantWhenTheRoleHasNoLease() {
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(1, CONTROLLER, "node-a", JOINED),
                        registration(2, CONTROLLER, "node-b", JOINED)));

        assertThat(Succession.evaluate(state, member("node-a"), JOINED, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.challenge());
        assertThat(Succession.evaluate(state, member("node-b"), JOINED, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.waitUntil(JOINED + 5_000));
        assertThat(Succession.evaluate(state, member("node-b"), JOINED + 5_000, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.challenge());
    }

    @Test
    void keepsTheAnchorFixedSoAStandbyBehindADeadRankZeroStillChallenges() {
        // An anchor of "now plus n staggers" would move forward on every evaluation, and rank 1
        // would then wait for ever while rank 0 is dead.
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(1, CONTROLLER, "node-a", JOINED),
                        registration(2, CONTROLLER, "node-b", JOINED)));

        for (long now = JOINED; now < JOINED + 5_000; now += 1_000) {
            assertThat(Succession.evaluate(state, member("node-b"), now, CONFIG).waitUntilMillis())
                    .isEqualTo(JOINED + 5_000);
        }
    }

    @Test
    void challengesAtOnceAfterALeaseTombstone() {
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(1, CONTROLLER, "node-a", JOINED),
                        lease(2, CONTROLLER, "node-a", GRANTED, DEADLINE),
                        leaseTombstone(3, CONTROLLER)));

        assertThat(Succession.evaluate(state, member("node-a"), GRANTED, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.challenge());
    }

    @Test
    void putsARecoveredNodeBehindTheMemberThatReplacedIt() {
        // node-a registered first, lost the role to node-b, and came back. Its new registration sits
        // at the tail, so it does not preempt node-b.
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(1, CONTROLLER, "node-a", JOINED),
                        registration(2, CONTROLLER, "node-b", JOINED),
                        lease(3, CONTROLLER, "node-b", GRANTED, DEADLINE),
                        registration(9, CONTROLLER, "node-a", GRANTED)));

        assertThat(state.rankOf(member("node-a"))).hasValue(0);
        assertThat(Succession.evaluate(state, member("node-b"), DEADLINE - 1, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.hold());
        assertThat(Succession.evaluate(state, member("node-a"), DEADLINE - 1, CONFIG))
                .usingRecursiveComparison()
                .isEqualTo(Decision.waitUntil(DEADLINE));
    }

    @Test
    void namesTheEarliestInstantAtWhichTheAnswerChanges() {
        RoleState state = threeCandidatesWithAHolder();
        Decision decision = Succession.evaluate(state, member("node-c"), GRANTED, CONFIG);

        assertThat(Succession.evaluate(state, member("node-c"), decision.waitUntilMillis() - 1, CONFIG)
                        .action())
                .isEqualTo(Decision.Action.WAIT);
        assertThat(Succession.evaluate(state, member("node-c"), decision.waitUntilMillis(), CONFIG)
                        .action())
                .isEqualTo(Decision.Action.CHALLENGE);
    }

    @Test
    void carriesNoWaitInstantForAnActionOtherThanWait() {
        assertThat(Decision.hold().waitUntilMillis()).isZero();
        assertThat(Decision.challenge().waitUntilMillis()).isZero();
        assertThat(Decision.notRegistered().waitUntilMillis()).isZero();
        assertThat(Decision.waitUntil(42L).waitUntilMillis()).isEqualTo(42L);
    }
}
