package io.krabka.streams.schema;

import org.apache.kafka.common.errors.RetriableException;

/** Signals that a background writer-schema fetch has not completed. */
public final class SchemaFetchPendingException extends RetriableException {
    private static final long serialVersionUID = 1L;

    private final int schemaId;

    public SchemaFetchPendingException(int schemaId) {
        super("writer schema for id " + Integer.toUnsignedString(schemaId) + " is pending fetch");
        this.schemaId = schemaId;
    }

    public int schemaId() {
        return schemaId;
    }
}
