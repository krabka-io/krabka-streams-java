package io.krabka.streams.schema;

/**
 * Reports a schema registry transport, status, or response error.
 *
 * <p>Thrown by {@link KrabkaSchemaRegistryClient} when a request cannot be sent, the
 * registry answers with a non-2xx status, or the response body cannot be parsed.
 * Because the client is asynchronous, the exception usually arrives as the cause of a
 * {@link java.util.concurrent.CompletionException}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * client.latest("orders-value").exceptionally(error -> {
 *     if (error.getCause() instanceof SchemaRegistryException registry
 *             && registry.statusCode() == 404) {
 *         return null; // subject does not exist yet
 *     }
 *     throw new CompletionException(error);
 * });
 * }</pre>
 */
public final class SchemaRegistryException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * The HTTP status code, or {@code -1} when no status was received.
     *
     * @serial the HTTP status code, or {@code -1} for a transport or parsing error
     */
    private final int statusCode;

    /**
     * Creates an exception for a malformed request or response.
     *
     * @param message a description of the failure
     */
    public SchemaRegistryException(String message) {
        this(message, -1, null);
    }

    /**
     * Creates an exception for a transport or encoding failure.
     *
     * @param message a description of the failure
     * @param cause the underlying error
     */
    public SchemaRegistryException(String message, Throwable cause) {
        this(message, -1, cause);
    }

    /**
     * Creates an exception for a non-2xx registry response.
     *
     * @param statusCode the HTTP status returned by the registry
     * @param body the response body, included in the message
     */
    public SchemaRegistryException(int statusCode, String body) {
        this("schema registry returned HTTP " + statusCode + ": " + body, statusCode, null);
    }

    private SchemaRegistryException(String message, int statusCode, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    /**
     * Returns the HTTP status, or {@code -1} for a transport or parsing error.
     *
     * @return the HTTP status code, or {@code -1} when no status was received
     */
    public int statusCode() {
        return statusCode;
    }
}
