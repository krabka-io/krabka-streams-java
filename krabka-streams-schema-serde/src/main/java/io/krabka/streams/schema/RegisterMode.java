package io.krabka.streams.schema;

/** Defines how prewarming resolves a schema ID. */
public enum RegisterMode {
    AUTO_REGISTER,
    LOOKUP_ONLY,
    USE_LATEST
}
