package io.krabka.streams.coordination;

import static io.krabka.streams.coordination.CoordinationRecords.COMPACTOR;
import static io.krabka.streams.coordination.CoordinationRecords.CONTROLLER;
import static io.krabka.streams.coordination.CoordinationRecords.deregistration;
import static io.krabka.streams.coordination.CoordinationRecords.lease;
import static io.krabka.streams.coordination.CoordinationRecords.leaseTombstone;
import static io.krabka.streams.coordination.CoordinationRecords.member;
import static io.krabka.streams.coordination.CoordinationRecords.registration;
import static io.krabka.streams.coordination.CoordinationRecords.token;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RoleStateTest {
    @Test
    void ordersTheRosterByRegistrationOffset() {
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(7, CONTROLLER, "node-b", 200),
                        registration(3, CONTROLLER, "node-a", 100),
                        registration(9, CONTROLLER, "node-c", 300)));

        assertThat(state.roster())
                .usingRecursiveComparison()
                .isEqualTo(List.of(
                        new RosterEntry(member("node-a"), 3, 100),
                        new RosterEntry(member("node-b"), 7, 200),
                        new RosterEntry(member("node-c"), 9, 300)));
    }

    @Test
    void movesARecoveredMemberToTheTailOfTheRoster() {
        // The no-failback rule. A recovered node registers again, takes a higher offset, and lands
        // behind the member that replaced it.
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(1, CONTROLLER, "node-a", 100),
                        registration(2, CONTROLLER, "node-b", 200),
                        registration(9, CONTROLLER, "node-a", 900)));

        assertThat(state.roster().stream().map(RosterEntry::member).toList())
                .containsExactly(member("node-b"), member("node-a"));
        assertThat(state.entry(member("node-a")))
                .usingRecursiveComparison()
                .isEqualTo(Optional.of(new RosterEntry(member("node-a"), 9, 900)));
    }

    @Test
    void keepsTheRecordOfTheHighestOffsetForEveryKey() {
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(9, CONTROLLER, "node-a", 900),
                        registration(1, CONTROLLER, "node-a", 100),
                        lease(10, CONTROLLER, "node-a", 900, 1_000),
                        lease(4, CONTROLLER, "node-a", 400, 500)));

        assertThat(state.entry(member("node-a")).orElseThrow().offset()).isEqualTo(9);
        assertThat(state.lease().orElseThrow().deadline()).isEqualTo(1_000);
    }

    @Test
    void removesAMemberOnARegistrationTombstone() {
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(1, CONTROLLER, "node-a", 100),
                        registration(2, CONTROLLER, "node-b", 200),
                        deregistration(3, CONTROLLER, "node-a")));

        assertThat(state.roster().stream().map(RosterEntry::member).toList())
                .containsExactly(member("node-b"));
        assertThat(state.entry(member("node-a"))).isEmpty();
    }

    @Test
    void doesNotReviveAMemberFromAnEarlierRegistration() {
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(deregistration(5, CONTROLLER, "node-a"), registration(1, CONTROLLER, "node-a", 100)));

        assertThat(state.roster()).isEmpty();
    }

    @Test
    void clearsTheLeaseOnALeaseTombstone() {
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(1, CONTROLLER, "node-a", 100),
                        lease(2, CONTROLLER, "node-a", 100, 30_100),
                        leaseTombstone(3, CONTROLLER)));

        assertThat(state.lease()).isEmpty();
        assertThat(state.holder()).isEmpty();
    }

    @Test
    void dropsEveryRecordOfAnotherRole() {
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(1, COMPACTOR, "node-z", 100),
                        lease(2, COMPACTOR, "node-z", 100, 30_100),
                        registration(3, CONTROLLER, "node-a", 300)));

        assertThat(state.roster().stream().map(RosterEntry::member).toList())
                .containsExactly(member("node-a"));
        assertThat(state.lease()).isEmpty();
    }

    @Test
    void namesTheHolderOfAnExpiredLease() {
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(registration(1, CONTROLLER, "node-a", 100), lease(2, CONTROLLER, "node-a", 100, 200)));

        assertThat(state.holder()).contains(member("node-a"));
        assertThat(state.lease())
                .usingRecursiveComparison()
                .isEqualTo(Optional.of(new Lease(member("node-a"), token((short) 1), 100, 200)));
    }

    @Test
    void ranksTheStandbysBehindTheHolderAndKeepsTheHolderAtRankZero() {
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(1, CONTROLLER, "node-a", 100),
                        registration(2, CONTROLLER, "node-b", 200),
                        registration(3, CONTROLLER, "node-c", 300),
                        lease(4, CONTROLLER, "node-b", 200, 30_200)));

        assertThat(state.rankOf(member("node-b"))).hasValue(0);
        assertThat(state.rankOf(member("node-a"))).hasValue(0);
        assertThat(state.rankOf(member("node-c"))).hasValue(1);
    }

    @Test
    void ranksInRosterOrderWhenNoMemberHoldsTheRole() {
        RoleState state = RoleState.fromRecords(
                CONTROLLER,
                List.of(
                        registration(1, CONTROLLER, "node-a", 100),
                        registration(2, CONTROLLER, "node-b", 200)));

        assertThat(state.rankOf(member("node-a"))).hasValue(0);
        assertThat(state.rankOf(member("node-b"))).hasValue(1);
    }

    @Test
    void ranksNoMemberThatDidNotRegister() {
        RoleState state = RoleState.fromRecords(CONTROLLER, List.of());

        assertThat(state.rankOf(member("node-a"))).isEmpty();
        assertThat(state.entry(member("node-a"))).isEmpty();
        assertThat(RoleState.empty().roster()).isEmpty();
    }

    @Test
    void foldsTheSameStateForAnyRecordOrder() {
        List<CoordinationEntry> records = List.of(
                registration(1, CONTROLLER, "node-a", 100),
                registration(2, CONTROLLER, "node-b", 200),
                lease(3, CONTROLLER, "node-a", 100, 30_100),
                deregistration(4, CONTROLLER, "node-b"));

        List<CoordinationEntry> reversed = new ArrayList<>(records);
        Collections.reverse(reversed);

        assertThat(RoleState.fromRecords(CONTROLLER, reversed))
                .usingRecursiveComparison()
                .isEqualTo(RoleState.fromRecords(CONTROLLER, records));
    }

    @Test
    void treatsTwoStatesOfOneRosterAndLeaseAsEqual() {
        List<CoordinationEntry> records =
                List.of(registration(1, CONTROLLER, "node-a", 100), lease(2, CONTROLLER, "node-a", 100, 200));

        assertThat(RoleState.fromRecords(CONTROLLER, records))
                .isEqualTo(RoleState.fromRecords(CONTROLLER, records))
                .hasSameHashCodeAs(RoleState.fromRecords(CONTROLLER, records))
                .isNotEqualTo(RoleState.empty());
        assertThat(RoleState.empty()).hasToString("RoleState[roster=[], lease=Optional.empty]");
    }

    @Test
    void reportsTheRoleItCollects() {
        assertThat(new RoleStateBuilder(CONTROLLER).role()).isEqualTo(CONTROLLER);
    }
}
