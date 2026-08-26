package io.krabka.streams.coordination;

import java.util.Objects;

/**
 * What one member does about a role right now.
 *
 * <p>{@link Succession#evaluate(RoleState, MemberId, long, LeaseConfig)} returns one.
 * The wait instant carries a value for {@link Action#WAIT} only, and it is zero for
 * every other action.
 *
 * <p>A decision never makes a member authoritative. It decides when a member calls
 * {@code InitProducerId}, and the transaction coordinator decides who wins. A wrong
 * decision makes a failover early or late, and it costs epoch churn.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * Decision decision = Succession.evaluate(state, me, clock.nowMillis(), config);
 * switch (decision.action()) {
 *     case NOT_REGISTERED -> transport.register(role, me);
 *     case CHALLENGE -> transport.acquireEpoch(role);
 *     case HOLD -> leadership.renew();
 *     case WAIT -> parkUntil(decision.waitUntilMillis());
 * }
 * }</pre>
 *
 * @param action the step the member takes now
 * @param waitUntilMillis the instant of the next evaluation, in milliseconds since the
 *     Unix epoch, for {@link Action#WAIT} only
 */
public record Decision(Action action, long waitUntilMillis) {
    /**
     * The step that one member takes about a role.
     *
     * <h2>Example</h2>
     *
     * <pre>{@code
     * if (decision.action() == Decision.Action.CHALLENGE) {
     *     transport.acquireEpoch(role);
     * }
     * }</pre>
     */
    public enum Action {
        /**
         * This member is not in the roster of the role, so it has no rank. It registers
         * first, reads the partition again, and evaluates again.
         */
        NOT_REGISTERED,

        /**
         * This member holds the role and its lease is live. It renews the lease, and it
         * calls {@code InitProducerId} for nothing.
         */
        HOLD,

        /** This member calls {@code InitProducerId} now. */
        CHALLENGE,

        /**
         * This member waits, and then it evaluates the role state again.
         * {@link Decision#waitUntilMillis()} carries the instant.
         */
        WAIT
    }

    /**
     * Rejects a null action.
     *
     * @param action the step the member takes now
     * @param waitUntilMillis the instant of the next evaluation, in milliseconds since
     *     the Unix epoch, for {@link Action#WAIT} only
     */
    public Decision {
        Objects.requireNonNull(action, "action");
    }

    /**
     * Returns the decision to register first.
     *
     * @return a decision that carries {@link Action#NOT_REGISTERED}
     */
    public static Decision notRegistered() {
        return new Decision(Action.NOT_REGISTERED, 0L);
    }

    /**
     * Returns the decision to keep the role.
     *
     * @return a decision that carries {@link Action#HOLD}
     */
    public static Decision hold() {
        return new Decision(Action.HOLD, 0L);
    }

    /**
     * Returns the decision to call {@code InitProducerId} now.
     *
     * @return a decision that carries {@link Action#CHALLENGE}
     */
    public static Decision challenge() {
        return new Decision(Action.CHALLENGE, 0L);
    }

    /**
     * Returns the decision to wait until one instant.
     *
     * @param untilMillis the instant of the next evaluation, in milliseconds since the
     *     Unix epoch
     * @return a decision that carries {@link Action#WAIT}
     */
    public static Decision waitUntil(long untilMillis) {
        return new Decision(Action.WAIT, untilMillis);
    }
}
