package io.krabka.streams.coordination;

/**
 * Reports that the broker refused a write, because another member holds the role now.
 *
 * <p>This is the expected end of a leadership. The transport throws it for broker error
 * code 47 {@code INVALID_PRODUCER_EPOCH} and for broker error code 90
 * {@code PRODUCER_FENCED}. A deposed leader learns that it lost the role from this
 * exception, and from nothing else. No clock takes part in the check.
 *
 * <p>The epoch that the member held is superseded, so the cluster already rejects every
 * write that the member makes. The caller stops the work of the role at once. A retry
 * of the same write cannot succeed.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try {
 *     leadership.renew();
 * } catch (FencedException lost) {
 *     controller.stop();
 * }
 * }</pre>
 */
public final class FencedException extends CoordinationException {
    private static final long serialVersionUID = 1L;

    /**
     * The name of the role another member took.
     *
     * @serial the role name, which every {@link Role} keeps well formed
     */
    private final String role;

    /**
     * Creates an exception for the role that this member no longer holds.
     *
     * @param role the role another member took
     * @param cause the broker error that reported the fence, or null when the fence
     *     came from a state comparison rather than from a write
     */
    public FencedException(Role role, Throwable cause) {
        super("fenced: another member holds role " + role, cause);
        this.role = role.name();
    }

    /**
     * Returns the role that this member no longer holds.
     *
     * @return the fenced role
     */
    public Role role() {
        return Role.of(role);
    }
}
