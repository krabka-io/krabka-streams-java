package io.krabka.streams.coordination;

import java.util.Objects;
import java.util.Optional;

/**
 * What one role looks like to a third party right now.
 *
 * <p>The status pairs the two independent answers about a role. The token is the pair
 * that Kafka's transaction coordinator reports for the {@code transactional.id} of the
 * role, and it is {@link FencingToken#NO_EPOCH} when no member has ever taken the role.
 * The state is the fold of the coordination topic, and it names the roster and the last
 * lease record.
 *
 * <p>{@link #current()} joins the two. It reports whether the member that wrote the
 * lease still holds the epoch of the role. A checker needs no membership and no lease
 * clock to answer that question. It is the check the design promises: "a third party
 * verifies a writer's authority with one request and no membership".
 *
 * <p>The lease deadline says nothing about authority. A holder past its deadline still
 * owns the newest epoch until a challenger mints a newer one.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * LeadershipStatus status = client.describe(Role.of("controller"));
 * System.out.println(status.holder().map(MemberId::id).orElse("none")
 *     + " holds " + status.token() + ", current=" + status.current());
 * }</pre>
 *
 * @param role the role the status describes
 * @param token the token the transaction coordinator reports for the role
 * @param state the folded state of the coordination topic for the role
 */
public record LeadershipStatus(Role role, FencingToken token, RoleState state) {
    /**
     * Rejects a null component.
     *
     * @param role the role the status describes
     * @param token the token the transaction coordinator reports for the role
     * @param state the folded state of the coordination topic for the role
     */
    public LeadershipStatus {
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(state, "state");
    }

    /**
     * Returns the member the last lease record names.
     *
     * @return the member that wrote the lease, and empty when the role has no lease
     */
    public Optional<MemberId> holder() {
        return state.holder();
    }

    /**
     * Returns the last lease record of the role.
     *
     * @return the lease, and empty when no member holds the role
     */
    public Optional<Lease> lease() {
        return state.lease();
    }

    /**
     * Reports whether any member has ever taken the role.
     *
     * @return true when the coordinator reports a minted token
     */
    public boolean held() {
        return token.minted();
    }

    /**
     * Reports whether the author of the lease still holds the epoch of the role.
     *
     * <p>A false answer means one of two things. Either no member holds the role, or a
     * challenger minted a newer epoch and has not written its own lease yet. In both
     * cases the broker already rejects every write that the author of this lease makes.
     *
     * @return true when the lease token equals the token of the coordinator
     */
    public boolean current() {
        return state.lease().filter(lease -> lease.token().equals(token)).isPresent();
    }
}
