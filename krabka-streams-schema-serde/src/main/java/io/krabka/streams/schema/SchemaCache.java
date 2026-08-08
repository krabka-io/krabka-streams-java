package io.krabka.streams.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Stores schema IDs and writer schemas for synchronous serde operations. */
public final class SchemaCache {
    private final KrabkaSchemaRegistryClient client;
    private final RegisterMode registerMode;
    private final SubjectNameStrategy subjectNameStrategy;
    private final ConcurrentMap<String, InternedSchema> interned = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Integer> subjectIds = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, String> writerSchemas = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, String> writerMessageTypes = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, CompletableFuture<?>> fetching = new ConcurrentHashMap<>();

    public SchemaCache(KrabkaSchemaRegistryClient client) {
        this(client, RegisterMode.AUTO_REGISTER, new TopicNameStrategy());
    }

    public SchemaCache(
            KrabkaSchemaRegistryClient client,
            RegisterMode registerMode,
            SubjectNameStrategy subjectNameStrategy) {
        this.client = Objects.requireNonNull(client, "client");
        this.registerMode = Objects.requireNonNull(registerMode, "registerMode");
        this.subjectNameStrategy = Objects.requireNonNull(subjectNameStrategy, "subjectNameStrategy");
    }

    public String subject(String topic, Role role) {
        return subjectNameStrategy.subject(topic, role);
    }

    /** Adds a local schema to the next prewarm operation. This operation is idempotent by subject. */
    public void intern(String subject, SchemaKind kind, String schema, String messageType) {
        interned.putIfAbsent(
                Objects.requireNonNull(subject, "subject"),
                new InternedSchema(kind, Objects.requireNonNull(schema, "schema"), messageType));
    }

    /** Resolves all interned subject IDs with the configured registration mode. */
    public CompletableFuture<Void> prewarm() {
        var futures = new ArrayList<CompletableFuture<Void>>(interned.size());
        interned.forEach((subject, local) -> futures.add(resolve(subject, local)));
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    public OptionalInt idForSubject(String subject) {
        var id = subjectIds.get(subject);
        return id == null ? OptionalInt.empty() : OptionalInt.of(id);
    }

    /**
     * Returns a cached writer schema. A cache miss starts one background fetch and throws a retriable error.
     */
    public String writerSchema(int schemaId) {
        var schema = writerSchemas.get(schemaId);
        if (schema != null) {
            return schema;
        }
        startWriterSchemaFetch(schemaId);
        throw new SchemaFetchPendingException(schemaId);
    }

    public String writerMessageType(int schemaId) {
        return writerMessageTypes.get(schemaId);
    }

    /** Adds a subject ID directly. This method supports deterministic tests and offline startup. */
    public void seedSubjectId(String subject, int schemaId) {
        subjectIds.put(subject, schemaId);
    }

    /** Adds a writer schema directly. This method supports deterministic tests and offline startup. */
    public void seedWriterSchema(int schemaId, String schema) {
        writerSchemas.put(schemaId, schema);
    }

    /** Adds Protobuf message metadata directly. */
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
        client.schemaById(schemaId).whenComplete((fetched, error) -> {
            if (error == null) {
                writerSchemas.put(schemaId, fetched.schema());
                if (fetched.messageType() != null) {
                    writerMessageTypes.put(schemaId, fetched.messageType());
                }
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
}
