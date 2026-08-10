package io.krabka.streams.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Stores schema IDs and writer schemas for synchronous serde operations.
 *
 * <p>Serdes must never block a Kafka poll loop on registry I/O, so all registry
 * traffic is funneled through this cache:
 *
 * <ol>
 *   <li>Each serde calls {@link #intern(String, SchemaKind, String, String)} (through
 *       its {@code registerSubject} method) to declare the subjects it will use.
 *   <li>The application calls {@link #prewarm()} once at startup, which resolves every
 *       interned subject to a schema ID with the configured {@link RegisterMode}.
 *   <li>Serialization then reads IDs synchronously with {@link #idForSubject(String)},
 *       and deserialization reads writer schemas with {@link #writerSchema(int)}; an
 *       unknown ID starts one background fetch and throws the retriable
 *       {@link SchemaFetchPendingException}.
 * </ol>
 *
 * <p>The cache is thread-safe and can be shared by any number of serdes; sharing one
 * cache per application is the intended usage.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var client = new KrabkaSchemaRegistryClient(URI.create("http://localhost:8081"));
 * var cache = new SchemaCache(client, RegisterMode.LOOKUP_ONLY, new TopicNameStrategy());
 *
 * keySerde.registerSubject("orders");
 * valueSerde.registerSubject("orders");
 *
 * var report = cache.prewarmReport().join();
 * if (!report.successful()) {
 *     report.failures().forEach((subject, error) ->
 *         log.error("cannot resolve {}", subject, error));
 * }
 * }</pre>
 */
public final class SchemaCache {
    private final KrabkaSchemaRegistryClient client;
    private final RegisterMode registerMode;
    private final SubjectNameStrategy subjectNameStrategy;
    private final ConcurrentMap<String, InternedSchema> interned = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Integer> subjectIds = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, String> writerSchemas = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, String> writerMessageTypes = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Map<String, String>> writerReferences = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, CompletableFuture<?>> fetching = new ConcurrentHashMap<>();

    /**
     * Creates a cache that auto-registers schemas and uses the topic naming rule.
     *
     * @param client the registry client used for prewarming and background fetches
     */
    public SchemaCache(KrabkaSchemaRegistryClient client) {
        this(client, RegisterMode.AUTO_REGISTER, new TopicNameStrategy());
    }

    /**
     * Creates a fully configured cache.
     *
     * @param client the registry client used for prewarming and background fetches
     * @param registerMode how prewarming resolves each interned subject to an ID
     * @param subjectNameStrategy the default topic-to-subject mapping
     */
    public SchemaCache(
            KrabkaSchemaRegistryClient client,
            RegisterMode registerMode,
            SubjectNameStrategy subjectNameStrategy) {
        this.client = Objects.requireNonNull(client, "client");
        this.registerMode = Objects.requireNonNull(registerMode, "registerMode");
        this.subjectNameStrategy = Objects.requireNonNull(subjectNameStrategy, "subjectNameStrategy");
    }

    /**
     * Maps a topic and role to a subject with the cache's default strategy.
     *
     * @param topic the Kafka topic name
     * @param role whether the schema describes the record key or value
     * @return the registry subject name
     */
    public String subject(String topic, Role role) {
        return subjectNameStrategy.subject(topic, role);
    }

    /**
     * Maps a topic and role to a subject with an explicit strategy.
     *
     * @param topic the Kafka topic name
     * @param role whether the schema describes the record key or value
     * @param strategy the strategy to use instead of the cache default
     * @return the registry subject name
     */
    public String subject(String topic, Role role, SubjectNameStrategy strategy) {
        return Objects.requireNonNull(strategy, "strategy").subject(topic, role);
    }

    /**
     * Adds a local schema to the next prewarm operation. This operation is idempotent
     * by subject.
     *
     * <p>Serdes call this through their {@code registerSubject} methods; call it
     * directly only when prewarming subjects that no local serde owns.
     *
     * @param subject the registry subject to resolve during prewarm
     * @param kind the schema format
     * @param schema the local schema text
     * @param messageType the Protobuf message full name, or null for other formats
     */
    public void intern(String subject, SchemaKind kind, String schema, String messageType) {
        interned.putIfAbsent(
                Objects.requireNonNull(subject, "subject"),
                new InternedSchema(kind, Objects.requireNonNull(schema, "schema"), messageType));
    }

    /**
     * Resolves all interned subject IDs with the configured registration mode.
     *
     * <p>All subjects are resolved concurrently. The returned future fails when any
     * subject fails, without reporting which one; use {@link #prewarmReport()} for
     * per-subject outcomes.
     *
     * @return a future that completes when every interned subject has resolved
     */
    public CompletableFuture<Void> prewarm() {
        var futures = new ArrayList<CompletableFuture<Void>>(interned.size());
        interned.forEach((subject, local) -> futures.add(resolve(subject, local)));
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Resolves every interned subject and reports successes and failures independently.
     *
     * <p>Unlike {@link #prewarm()}, the returned future always completes normally;
     * inspect the report to find out which subjects failed and why. Successfully
     * resolved subjects are cached even when others fail.
     *
     * @return a future with the per-subject resolution report
     */
    public CompletableFuture<PrewarmReport> prewarmReport() {
        var resolved = new ConcurrentHashMap<String, Integer>();
        var failures = new ConcurrentHashMap<String, Throwable>();
        var futures = new ArrayList<CompletableFuture<Void>>(interned.size());
        interned.forEach((subject, local) -> futures.add(resolve(subject, local).handle((ignored, error) -> {
            if (error == null) {
                resolved.put(subject, subjectIds.get(subject));
            } else {
                failures.put(subject, error instanceof java.util.concurrent.CompletionException && error.getCause() != null
                        ? error.getCause()
                        : error);
            }
            return null;
        })));
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> new PrewarmReport(resolved, failures));
    }

    /**
     * Returns the resolved schema ID for a subject, if prewarming has resolved it.
     *
     * @param subject the registry subject name
     * @return the schema ID, or empty when the subject has not been resolved
     */
    public OptionalInt idForSubject(String subject) {
        var id = subjectIds.get(subject);
        return id == null ? OptionalInt.empty() : OptionalInt.of(id);
    }

    /**
     * Returns a cached writer schema. A cache miss starts one background fetch and
     * throws a retriable error.
     *
     * <p>This method never blocks. Only one fetch per schema ID runs at a time;
     * concurrent callers for the same ID all receive
     * {@link SchemaFetchPendingException} until the fetch completes.
     *
     * @param schemaId the schema ID from a record's frame header
     * @return the writer schema text registered under the ID
     * @throws SchemaFetchPendingException while the schema is being fetched
     */
    public String writerSchema(int schemaId) {
        var schema = writerSchemas.get(schemaId);
        if (schema != null) {
            return schema;
        }
        startWriterSchemaFetch(schemaId);
        throw new SchemaFetchPendingException(schemaId);
    }

    /**
     * Returns the cached Protobuf message full name for a schema ID.
     *
     * @param schemaId the schema ID from a record's frame header
     * @return the writer's message full name, or null when unknown or not Protobuf
     */
    public String writerMessageType(int schemaId) {
        return writerMessageTypes.get(schemaId);
    }

    /**
     * Returns the cached referenced schemas for a schema ID.
     *
     * @param schemaId the schema ID from a record's frame header
     * @return reference name to schema text, empty when the schema has no references
     */
    public Map<String, String> writerReferences(int schemaId) {
        return writerReferences.getOrDefault(schemaId, Map.of());
    }

    /**
     * Adds a subject ID directly. This method supports deterministic tests and offline
     * startup.
     *
     * @param subject the registry subject name
     * @param schemaId the schema ID to associate with the subject
     */
    public void seedSubjectId(String subject, int schemaId) {
        subjectIds.put(subject, schemaId);
    }

    /**
     * Adds a writer schema directly. This method supports deterministic tests and
     * offline startup.
     *
     * @param schemaId the schema ID to associate with the schema text
     * @param schema the writer schema text
     */
    public void seedWriterSchema(int schemaId, String schema) {
        writerSchemas.put(schemaId, schema);
    }

    /**
     * Adds Protobuf message metadata directly.
     *
     * @param schemaId the schema ID to associate with the message name
     * @param messageType the Protobuf message full name
     */
    public void seedWriterMessageType(int schemaId, String messageType) {
        writerMessageTypes.put(schemaId, messageType);
    }

    private CompletableFuture<Void> resolve(String subject, InternedSchema local) {
        CompletableFuture<Resolution> resolution;
        switch (registerMode) {
            case AUTO_REGISTER -> resolution = client.register(subject, local.kind(), local.schema(), local.messageType())
                    .thenApply(id -> new Resolution(id, local.messageType()));
            case LOOKUP_ONLY -> resolution = client.lookup(subject, local.kind(), local.schema(), local.messageType())
                    .thenApply(id -> new Resolution(id, local.messageType()));
            case USE_LATEST -> resolution = client.latest(subject)
                    .thenApply(latest -> new Resolution(latest.id(), latest.messageType()));
            default -> throw new IllegalStateException("unknown register mode " + registerMode);
        }
        return resolution.thenAccept(resolved -> {
            subjectIds.put(subject, resolved.id());
            writerSchemas.put(resolved.id(), local.schema());
            if (resolved.messageType() != null) {
                writerMessageTypes.put(resolved.id(), resolved.messageType());
            }
        });
    }

    private void startWriterSchemaFetch(int schemaId) {
        var marker = new CompletableFuture<Void>();
        if (fetching.putIfAbsent(schemaId, marker) != null) {
            return;
        }
        client.resolvedSchemaById(schemaId).whenComplete((fetched, error) -> {
            if (error == null) {
                writerSchemas.put(schemaId, fetched.schema());
                if (fetched.messageType() != null) {
                    writerMessageTypes.put(schemaId, fetched.messageType());
                }
                writerReferences.put(schemaId, fetched.references());
                marker.complete(null);
            } else {
                marker.completeExceptionally(error);
            }
            fetching.remove(schemaId, marker);
        });
    }

    private record InternedSchema(SchemaKind kind, String schema, String messageType) {
        private InternedSchema {
            Objects.requireNonNull(kind, "kind");
        }
    }

    private record Resolution(int id, String messageType) {
    }

    /**
     * The per-subject outcome of {@link SchemaCache#prewarmReport()}.
     *
     * @param resolved subject to resolved schema ID for every successful subject
     * @param failures subject to failure cause for every failed subject
     */
    public record PrewarmReport(Map<String, Integer> resolved, Map<String, Throwable> failures) {
        /**
         * Copies both maps so the report is immutable.
         *
         * @param resolved subject to resolved schema ID for every successful subject
         * @param failures subject to failure cause for every failed subject
         */
        public PrewarmReport {
            resolved = Map.copyOf(resolved);
            failures = Map.copyOf(failures);
        }

        /**
         * Returns whether every subject resolved.
         *
         * @return true when there are no failures
         */
        public boolean successful() {
            return failures.isEmpty();
        }
    }
}
