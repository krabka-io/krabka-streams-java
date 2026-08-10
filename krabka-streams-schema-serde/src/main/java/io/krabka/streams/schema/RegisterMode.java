package io.krabka.streams.schema;

/**
 * Defines how prewarming resolves a schema ID.
 *
 * <p>{@link SchemaCache#prewarm()} resolves every interned subject to a schema ID by
 * talking to the registry once. The mode chooses the registry operation used for that
 * resolution.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * // Production: never create new schema versions from application startup.
 * var cache = new SchemaCache(client, RegisterMode.LOOKUP_ONLY, new TopicNameStrategy());
 * serde.registerSubject("orders");
 * cache.prewarm().join();
 * }</pre>
 */
public enum RegisterMode {
    /**
     * Registers the local schema, creating a new subject version when the schema is
     * unknown to the registry. Registration is idempotent: re-registering an existing
     * schema returns its existing ID.
     */
    AUTO_REGISTER,

    /**
     * Looks the local schema up under the subject and fails prewarming when the
     * registry does not already know it. Use this in environments where schemas are
     * registered by a deployment pipeline rather than by applications.
     */
    LOOKUP_ONLY,

    /**
     * Uses the latest registered version of the subject regardless of the local
     * schema text. The writer schema recorded for the resolved ID is still the local
     * schema, so the local and latest schemas must be compatible.
     */
    USE_LATEST
}
