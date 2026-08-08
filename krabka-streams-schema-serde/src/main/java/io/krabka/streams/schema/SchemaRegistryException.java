package io.krabka.streams.schema;

/** Reports a schema registry transport, status, or response error. */
public final class SchemaRegistryException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final int statusCode;

    public SchemaRegistryException(String message) {
        this(message, -1, null);
    }

    public SchemaRegistryException(String message, Throwable cause) {
        this(message, -1, cause);
    }

    public SchemaRegistryException(int statusCode, String body) {
        this("schema registry returned HTTP " + statusCode + ": " + body, statusCode, null);
    }

    private SchemaRegistryException(String message, int statusCode, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    /** Returns the HTTP status, or {@code -1} for a transport or parsing error. */
    public int statusCode() {
        return statusCode;
    }
}
