package io.krabka.streams.coordination;

/**
 * Reports a coordination failure.
 *
 * <p>This is the module's unchecked exception type. The record codec throws it for a
 * malformed key or value and for a name that breaks its bounds. {@link LeaseConfig}
 * throws it for a set of timings that cannot work together. The transport throws it
 * for a broker call that fails. {@link FencedException} is the one subclass, and it
 * reports the single failure a caller must treat as the end of a leadership.
 *
 * <p>The message text names the record part or the field that failed, so a failure
 * identifies itself without a debugger.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * assertThatThrownBy(() -> Role.of(""))
 *     .isInstanceOf(CoordinationException.class)
 *     .hasMessage("a coordination role must not be empty");
 * }</pre>
 */
public class CoordinationException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates an exception with a message.
     *
     * @param message a description of the failure
     */
    public CoordinationException(String message) {
        super(message);
    }

    /**
     * Creates an exception with a message and a cause.
     *
     * @param message a description of the failure
     * @param cause the underlying error
     */
    public CoordinationException(String message, Throwable cause) {
        super(message, cause);
    }
}
