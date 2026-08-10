package io.krabka.streams.columnar;

/**
 * Reports an Arrow codec, topology, or operator failure.
 *
 * <p>This is the module's single unchecked exception type: codecs throw it for
 * undecodable records and oversized rows, topologies for structural problems such as
 * duplicate node names, and operators for missing columns or unrestorable state. The
 * message text is stable enough to assert on in tests.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * assertThatThrownBy(() -> codec.decode(List.of()))
 *     .isInstanceOf(ColumnarException.class)
 *     .hasMessage("decode called with an empty record batch");
 * }</pre>
 */
public final class ColumnarException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates an exception with a message.
     *
     * @param message a description of the failure
     */
    public ColumnarException(String message) {
        super(message);
    }

    /**
     * Creates an exception with a message and cause.
     *
     * @param message a description of the failure
     * @param cause the underlying error
     */
    public ColumnarException(String message, Throwable cause) {
        super(message, cause);
    }
}
