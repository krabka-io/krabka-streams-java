package io.krabka.streams.coordination;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * The succession rules of one role: ordered candidates, and no failback.
 *
 * <p>The rules are pure. They read a {@link RoleState}, one instant, and a
 * {@link LeaseConfig}, and they perform no input and no output. A test drives a whole
 * failover with no broker and no sleep.
 *
 * <h2>The epoch is the safety mechanism</h2>
 *
 * <p>Kafka's transaction coordinator mints the leadership epoch when a member calls
 * {@code InitProducerId} for the {@code transactional.id} of a role. The quorum picks
 * the value, the value only grows, and every broker rejects a write that carries an
 * older one. That fence is the whole of the safety story. The rules here decide
 * <em>when</em> a member calls {@code InitProducerId}. They never decide who is
 * authoritative, because the coordinator decides that. A wrong decision makes a
 * failover early or late, and it costs epoch churn. It never makes two members
 * authoritative for one role.
 *
 * <h2>Rank comes from the log, not from configuration</h2>
 *
 * <p>A candidate appends a registration record to {@link CoordinationCodec#TOPIC}. The
 * offset of that record in the partition is the join sequence of the candidate. Log
 * compaction keeps the offset of every record it retains, so a reader that walks the
 * partition in offset order sees the registrations in registration order. A recovered
 * node registers again, takes a higher offset, and lands at the tail of the roster.
 * That is the no-failback rule, and it needs no counter and no coordinator.
 *
 * <h2>The stagger is an optimisation</h2>
 *
 * <p>A challenger of rank {@code n} waits {@code n} challenge staggers past the deadline
 * of the lease. This saves epoch churn, and it does nothing else.
 * {@code InitProducerId} is atomic at the coordinator, so a simultaneous challenge by
 * every standby still leaves exactly one member with the newest epoch. The losers keep
 * an older epoch, and each one learns that it lost on its first guarded write. Set the
 * stagger to make that outcome rare. Do not set it to make the outcome safe, because
 * the outcome is already safe.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * RoleState state = RoleState.fromRecords(role, transport.readRoleRecords(role));
 * Decision decision = Succession.evaluate(state, me, clock.nowMillis(), config);
 * if (decision.action() == Decision.Action.CHALLENGE) {
 *     FencingToken token = transport.acquireEpoch(role);
 * }
 * }</pre>
 */
public final class Succession {
    private Succession() {
    }

    /**
     * Decides what one member does about a role at one instant.
     *
     * <p>The rules are:
     *
     * <ol>
     *   <li>A member that is not in the roster gets {@link Decision.Action#NOT_REGISTERED}.
     *       It has no rank, because rank comes from the registration record.
     *   <li>A member that the lease names, while that lease is live, gets
     *       {@link Decision.Action#HOLD}.
     *   <li>A challenger of rank {@code n} gets {@link Decision.Action#CHALLENGE} from
     *       the deadline plus {@code n} challenge staggers on. With no lease, the anchor
     *       is the registration instant of the member instead of a deadline, so rank 0
     *       challenges at once and rank {@code n} challenges {@code n} staggers later.
     *   <li>Every other member gets {@link Decision.Action#WAIT}. The instant it carries
     *       is the earliest instant at which this answer changes for an unchanged role
     *       state.
     * </ol>
     *
     * <p>The anchor of rule 3 always comes from the role state, and never from the
     * current instant. A member that has no lease to wait for anchors on its own
     * registration record. An anchor of "now plus {@code n} staggers" would move forward
     * on every evaluation, and a standby of rank 1 or more would then wait for ever
     * while rank 0 is dead.
     *
     * <p>The member of an expired lease keeps rank 0, so it reclaims its own role at its
     * own deadline. See {@link RoleState#rankOf(MemberId)}.
     *
     * <p>A caller evaluates again when it reads a new record, and at the latest at the
     * instant that {@link Decision#waitUntilMillis()} names.
     *
     * @param state the folded state of the role
     * @param member the member that asks
     * @param nowMillis the instant to decide at, in milliseconds since the Unix epoch
     * @param config the lease timings of the role
     * @return the step the member takes now
     * @throws NullPointerException if the state, the member, or the config is null
     */
    public static Decision evaluate(
            RoleState state, MemberId member, long nowMillis, LeaseConfig config) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(member, "member");
        Objects.requireNonNull(config, "config");
        Optional<RosterEntry> entry = state.entry(member);
        OptionalInt rank = state.rankOf(member);
        if (entry.isEmpty() || rank.isEmpty()) {
            return Decision.notRegistered();
        }
        long challengeAtMillis;
        Optional<Lease> lease = state.lease();
        if (lease.isPresent()) {
            LeaseTiming timing = config.timing(lease.orElseThrow());
            if (lease.orElseThrow().member().equals(member) && timing.liveAt(nowMillis)) {
                return Decision.hold();
            }
            challengeAtMillis = timing.challengeAtMillis(rank.orElseThrow());
        } else {
            challengeAtMillis = Instants.add(
                    entry.orElseThrow().registeredAt(), config.challengeDelayMillis(rank.orElseThrow()));
        }
        return nowMillis >= challengeAtMillis
                ? Decision.challenge()
                : Decision.waitUntil(challengeAtMillis);
    }
}
