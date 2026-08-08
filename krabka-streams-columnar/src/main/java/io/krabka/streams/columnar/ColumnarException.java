package io.krabka.streams.columnar;

/** Reports an Arrow codec, topology, or operator failure. */
public final class ColumnarException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ColumnarException(String message) {
        super(message);
    }

    public ColumnarException(String message, Throwable cause) {
        super(message, cause);
    }
}
