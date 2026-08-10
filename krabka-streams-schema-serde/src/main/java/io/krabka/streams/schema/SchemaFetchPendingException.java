package io.krabka.streams.schema;

import org.apache.kafka.common.errors.RetriableException;

/**
 * Signals that a background writer-schema fetch has not completed.
 *
 * <p>{@link SchemaCache#writerSchema(int)} never blocks. When a deserializer meets a
 * schema ID that is not cached yet, the cache starts one asynchronous registry fetch
 * and throws this exception. It extends Kafka's {@link RetriableException}, so a
 * consumer or streams runtime retries the record; by then the fetch has usually
 * completed and deserialization succeeds.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try {
 *     var order = serde.deserializer().deserialize("orders", bytes);
 * } catch (SchemaFetchPendingException pending) {
 *     // Schema id is being fetched in the background; retry the record shortly.
 *     retryLater(pending.schemaId());
 * }
 * }</pre>
 */
public final class SchemaFetchPendingException extends RetriableException {
    private static final long serialVersionUID = 1L;

    /**
     * The schema ID whose writer schema is being fetched.
     *
     * @serial the pending schema ID
     */
    private final int schemaId;

    /**
     * Creates the exception for one pending schema ID.
     *
     * @param schemaId the schema ID whose writer schema is being fetched
     */
    public SchemaFetchPendingException(int schemaId) {
        super("writer schema for id " + Integer.toUnsignedString(schemaId) + " is pending fetch");
        this.schemaId = schemaId;
    }

    /**
     * Returns the schema ID whose writer schema is being fetched.
     *
     * @return the pending schema ID
     */
    public int schemaId() {
        return schemaId;
    }
}
