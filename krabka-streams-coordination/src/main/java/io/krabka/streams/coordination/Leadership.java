package io.krabka.streams.coordination;

/**
 * The live leadership of one role by one member.
 *
 * <p>{@link CoordinationClient#tryAcquire(Role, MemberId)} returns one after the member
 * minted the epoch of the role and wrote its first lease. The handle carries the token,
 * and the caller passes that token to every guarded write. The broker checks it, so a
 * leader does not have to prove that its lease is live before each write. The write
 * itself carries the proof.
 *
 * <p>{@link #renew()} writes a fresh lease record under the same token. It throws
 * {@link FencedException} when another member has taken the role, and that exception is
 * the only signal that a leadership ended. A caller stops the work of the role at once
 * and does not retry.
 *
 * <p>The handle is {@link AutoCloseable}. {@link #close()} resigns, so a standby
 * challenges at once rather than at the deadline of the record this member left behind.
 * A close after a fence is a no-op, because the cluster already moved the role.
 *
 * <p>One handle is not safe for concurrent use. Confine it to the thread that runs the
 * work of the role.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try (Leadership leadership = client.acquire(role, me, Duration.ofMinutes(1))) {
 *     while (running) {
 *         if (leadership.renewDue()) {
 *             leadership.renew();
 *         }
 *         dispatch(leadership.token());
 *     }
 * } catch (FencedException lost) {
 *     controller.stop();
 * }
 * }</pre>
 */
public final class Leadership implements AutoCloseable {
    private final CoordinationTransport transport;
    private final LeaseConfig config;
    private final Clock clock;
    private final Role role;
    private final MemberId member;
    private final FencingToken token;

    private Lease lease;
    private boolean held = true;

    Leadership(CoordinationTransport transport, LeaseConfig config, Clock clock, Role role,
            MemberId member, FencingToken token, Lease lease) {
        this.transport = transport;
        this.config = config;
        this.clock = clock;
        this.role = role;
        this.member = member;
        this.token = token;
        this.lease = lease;
    }

    /**
     * Returns the role this member holds.
     *
     * @return the role of this leadership
     */
    public Role role() {
        return role;
    }

    /**
     * Returns the member that holds the role.
     *
     * @return the member of this leadership
     */
    public MemberId member() {
        return member;
    }

    /**
     * Returns the quorum-minted proof of this leadership.
     *
     * <p>Pass this token to every guarded write. The broker rejects a write that carries
     * an older token.
     *
     * @return the token the transaction coordinator minted for this member
     */
    public FencingToken token() {
        return token;
    }

    /**
     * Returns the last lease record this member wrote.
     *
     * @return the current lease of the role
     */
    public Lease lease() {
        return lease;
    }

    /**
     * Returns the lease clock of the current lease.
     *
     * @return the clock questions this policy answers about the current lease
     */
    public LeaseTiming timing() {
        return config.timing(lease);
    }

    /**
     * Reports whether this handle still believes it holds the role.
     *
     * <p>The answer turns false after a fence and after a resignation. It says nothing
     * about the lease deadline, because the deadline is not the authority.
     *
     * @return true until this member is fenced or resigns
     */
    public boolean held() {
        return held;
    }

    /**
     * Reports whether the holder writes a renewal now.
     *
     * @return true when the renewal instant of the current lease has passed
     */
    public boolean renewDue() {
        return timing().renewDueAt(clock.nowMillis());
    }

    /**
     * Writes a fresh lease record under the same token.
     *
     * <p>The record carries the same token and a later grant instant, so a renewal and a
     * first grant are the same write. The broker rejects the write when another member
     * has taken the role.
     *
     * @return the lease record this call wrote
     * @throws FencedException if another member holds the role now
     * @throws CoordinationException if this member already resigned, or if the write
     *     fails for another reason
     */
    public Lease renew() {
        requireHeld();
        Lease renewed = config.grant(member, token, clock.nowMillis());
        try {
            transport.writeLease(role, token, renewed);
        } catch (FencedException fenced) {
            held = false;
            throw fenced;
        }
        lease = renewed;
        return renewed;
    }

    /**
     * Gives up the role and clears the lease record.
     *
     * <p>A standby then challenges at once rather than at the deadline of the record
     * this member left behind. The epoch of this member stays valid until a challenger
     * mints a newer one, so a resignation is a courtesy and not a fence.
     *
     * @throws FencedException if another member already holds the role
     * @throws CoordinationException if this member already resigned, or if the write
     *     fails for another reason
     */
    public void resign() {
        requireHeld();
        try {
            transport.clearLease(role, token);
        } finally {
            held = false;
        }
    }

    /**
     * Resigns when this member still holds the role.
     *
     * <p>The method swallows a fence, because a fenced member has nothing left to give
     * up. It rethrows every other failure.
     *
     * @throws CoordinationException if the tombstone write fails for a reason other than
     *     a fence
     */
    @Override
    public void close() {
        if (!held) {
            return;
        }
        try {
            resign();
        } catch (FencedException alreadyLost) {
            held = false;
        }
    }

    private void requireHeld() {
        if (!held) {
            throw new CoordinationException(
                    "member " + member + " no longer holds role " + role);
        }
    }
}
