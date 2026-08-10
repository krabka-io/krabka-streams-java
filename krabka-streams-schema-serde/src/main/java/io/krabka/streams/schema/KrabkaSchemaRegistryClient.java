package io.krabka.streams.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A nonblocking client for the Confluent Schema Registry REST API.
 *
 * <p>Every operation returns a {@link CompletableFuture} and no method blocks the
 * calling thread; requests run on the {@link HttpClient}'s executor. Transport errors,
 * HTTP 429, and 5xx responses are retried up to the configured retry count. Failed
 * futures complete with {@link SchemaRegistryException} (usually wrapped in a
 * {@link CompletionException}).
 *
 * <p>Applications rarely call this client directly for serialization; they hand it to
 * a {@link SchemaCache}, which layers caching and prewarming on top. Direct use suits
 * tooling: registering schemas from a pipeline, checking compatibility levels, or
 * cleaning up subjects.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var client = new KrabkaSchemaRegistryClient(URI.create("http://localhost:8081"));
 *
 * int id = client.register("orders-value", SchemaKind.AVRO, orderSchema, null).join();
 * var latest = client.latest("orders-value").join();
 * List<String> subjects = client.subjects().join();
 * }</pre>
 */
public final class KrabkaSchemaRegistryClient {
    private static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    private final URI baseUri;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final int maxRetries;
    private final String authorization;

    /**
     * Creates a client with a default HTTP client, default mapper, and two retries.
     *
     * @param baseUri the registry base URI, for example {@code http://localhost:8081}
     */
    public KrabkaSchemaRegistryClient(URI baseUri) {
        this(baseUri, HttpClient.newHttpClient(), new ObjectMapper(), 2, null);
    }

    /**
     * Creates a client with caller-supplied HTTP client and mapper and two retries.
     *
     * @param baseUri the registry base URI
     * @param httpClient the HTTP client that executes requests
     * @param objectMapper the mapper for request and response JSON
     */
    public KrabkaSchemaRegistryClient(URI baseUri, HttpClient httpClient, ObjectMapper objectMapper) {
        this(baseUri, httpClient, objectMapper, 2, null);
    }

    /**
     * Creates a client with an explicit retry budget.
     *
     * @param baseUri the registry base URI
     * @param httpClient the HTTP client that executes requests
     * @param objectMapper the mapper for request and response JSON
     * @param maxRetries how many times a failed request is retried; zero disables retries
     * @throws IllegalArgumentException if {@code maxRetries} is negative
     */
    public KrabkaSchemaRegistryClient(
            URI baseUri, HttpClient httpClient, ObjectMapper objectMapper, int maxRetries) {
        this(baseUri, httpClient, objectMapper, maxRetries, null);
    }

    /**
     * Creates a client that sends HTTP basic authentication with every request.
     *
     * @param baseUri the registry base URI
     * @param username the basic-auth user name
     * @param password the basic-auth password
     */
    public KrabkaSchemaRegistryClient(URI baseUri, String username, String password) {
        this(
                baseUri,
                HttpClient.newHttpClient(),
                new ObjectMapper(),
                2,
                "Basic " + Base64.getEncoder().encodeToString(
                        (Objects.requireNonNull(username, "username") + ":"
                                        + Objects.requireNonNull(password, "password"))
                                .getBytes(StandardCharsets.UTF_8)));
    }

    private KrabkaSchemaRegistryClient(
            URI baseUri,
            HttpClient httpClient,
            ObjectMapper objectMapper,
            int maxRetries,
            String authorization) {
        var normalized = Objects.requireNonNull(baseUri, "baseUri").toString().replaceAll("/+$", "") + "/";
        this.baseUri = URI.create(normalized);
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative");
        }
        this.maxRetries = maxRetries;
        this.authorization = authorization;
    }

    /**
     * Registers a schema under a subject without references.
     *
     * <p>Registration is idempotent: registering a schema the subject already has
     * returns the existing ID.
     *
     * @param subject the registry subject name
     * @param kind the schema format
     * @param schema the schema text
     * @param messageType the Protobuf message full name, or null for other formats
     * @return a future with the registered schema's ID
     */
    public CompletableFuture<Integer> register(
            String subject, SchemaKind kind, String schema, String messageType) {
        return register(subject, kind, schema, messageType, List.of());
    }

    /**
     * Registers a schema under a subject with schema references.
     *
     * @param subject the registry subject name
     * @param kind the schema format
     * @param schema the schema text
     * @param messageType the Protobuf message full name, or null for other formats
     * @param references the schemas this schema refers to
     * @return a future with the registered schema's ID
     */
    public CompletableFuture<Integer> register(
            String subject,
            SchemaKind kind,
            String schema,
            String messageType,
            List<SchemaReference> references) {
        return post(
                        "/subjects/" + pathSegment(subject) + "/versions",
                        payload(kind, schema, messageType, references))
                .thenApply(response -> requiredInt(response, "id"));
    }

    /**
     * Looks up the ID of an already registered schema without references.
     *
     * @param subject the registry subject name
     * @param kind the schema format
     * @param schema the schema text to look up
     * @param messageType the Protobuf message full name, or null for other formats
     * @return a future with the schema's ID; fails with HTTP 404 when unregistered
     */
    public CompletableFuture<Integer> lookup(
            String subject, SchemaKind kind, String schema, String messageType) {
        return lookup(subject, kind, schema, messageType, List.of());
    }

    /**
     * Looks up the ID of an already registered schema with schema references.
     *
     * @param subject the registry subject name
     * @param kind the schema format
     * @param schema the schema text to look up
     * @param messageType the Protobuf message full name, or null for other formats
     * @param references the schemas this schema refers to
     * @return a future with the schema's ID; fails with HTTP 404 when unregistered
     */
    public CompletableFuture<Integer> lookup(
            String subject,
            SchemaKind kind,
            String schema,
            String messageType,
            List<SchemaReference> references) {
        return post("/subjects/" + pathSegment(subject), payload(kind, schema, messageType, references))
                .thenApply(response -> requiredInt(response, "id"));
    }

    /**
     * Fetches the latest registered version of a subject.
     *
     * @param subject the registry subject name
     * @return a future with the latest version's metadata and schema text
     */
    public CompletableFuture<RegisteredSchema> latest(String subject) {
        return get("/subjects/" + pathSegment(subject) + "/versions/latest")
                .thenApply(KrabkaSchemaRegistryClient::registeredSchema);
    }

    /**
     * Fetches the schema ID of a subject's latest version.
     *
     * @param subject the registry subject name
     * @return a future with the latest version's schema ID
     */
    public CompletableFuture<Integer> latestId(String subject) {
        return latest(subject).thenApply(RegisteredSchema::id);
    }

    /**
     * Fetches a schema by its global ID.
     *
     * @param schemaId the registry schema ID
     * @return a future with the schema text and reference list; references are not
     *     resolved to their own schema texts
     */
    public CompletableFuture<FetchedSchema> schemaById(int schemaId) {
        return get("/schemas/ids/" + Integer.toUnsignedString(schemaId))
                .thenApply(node -> new FetchedSchema(
                        requiredText(node, "schema"), optionalText(node, "messageType"), references(node)));
    }

    /**
     * Fetches a schema by ID and recursively resolves its references.
     *
     * <p>Each referenced subject version is fetched, and its own references are
     * resolved transitively. Reference cycles are tolerated: a subject version already
     * being resolved on the current path is skipped.
     *
     * @param schemaId the registry schema ID
     * @return a future with the schema text and every transitive reference's text
     */
    public CompletableFuture<ResolvedSchema> resolvedSchemaById(int schemaId) {
        return schemaById(schemaId).thenCompose(this::resolveReferences);
    }

    private CompletableFuture<ResolvedSchema> resolveReferences(FetchedSchema schema) {
        return resolveReferences(schema.references(), java.util.Set.of())
                .thenApply(references -> new ResolvedSchema(schema.schema(), schema.messageType(), references));
    }

    private CompletableFuture<java.util.Map<String, String>> resolveReferences(
            List<SchemaReference> references, java.util.Set<String> ancestors) {
        CompletableFuture<java.util.Map<String, String>> result =
                CompletableFuture.completedFuture(new java.util.LinkedHashMap<>());
        for (var reference : references) {
            var key = reference.subject() + "@" + reference.version();
            if (ancestors.contains(key)) {
                continue;
            }
            var nextAncestors = new java.util.HashSet<>(ancestors);
            nextAncestors.add(key);
            result = result.thenCombine(
                    version(reference.subject(), reference.version()).thenCompose(value ->
                            resolveReferences(value.references(), nextAncestors).thenApply(nested -> {
                                var resolved = new java.util.LinkedHashMap<>(nested);
                                resolved.put(reference.name(), value.schema());
                                return resolved;
                            })),
                    (resolved, next) -> {
                        resolved.putAll(next);
                        return resolved;
                    });
        }
        return result;
    }

    /**
     * Lists all subject names.
     *
     * @return a future with every subject known to the registry
     */
    public CompletableFuture<List<String>> subjects() {
        return get("/subjects").thenApply(node -> stream(node).map(JsonNode::asText).toList());
    }

    /**
     * Lists a subject's version numbers.
     *
     * @param subject the registry subject name
     * @return a future with the subject's version numbers in registry order
     */
    public CompletableFuture<List<Integer>> versions(String subject) {
        return get("/subjects/" + pathSegment(subject) + "/versions")
                .thenApply(node -> stream(node).map(JsonNode::intValue).toList());
    }

    /**
     * Fetches one specific version of a subject.
     *
     * @param subject the registry subject name
     * @param version the version number to fetch
     * @return a future with the version's metadata and schema text
     */
    public CompletableFuture<RegisteredSchema> version(String subject, int version) {
        return get("/subjects/" + pathSegment(subject) + "/versions/" + version)
                .thenApply(KrabkaSchemaRegistryClient::registeredSchema);
    }

    /**
     * Reads a subject's compatibility level.
     *
     * @param subject the registry subject name
     * @return a future with the compatibility level, for example {@code BACKWARD}
     */
    public CompletableFuture<String> compatibility(String subject) {
        return get("/config/" + pathSegment(subject)).thenApply(node -> requiredText(node, "compatibilityLevel"));
    }

    /**
     * Reads the registry's global compatibility level.
     *
     * @return a future with the global compatibility level
     */
    public CompletableFuture<String> compatibility() {
        return get("/config").thenApply(node -> requiredText(node, "compatibilityLevel"));
    }

    /**
     * Sets a subject's compatibility level.
     *
     * @param subject the registry subject name
     * @param level the level to set, for example {@code BACKWARD} or {@code FULL}
     * @return a future with the level the registry acknowledged
     */
    public CompletableFuture<String> setCompatibility(String subject, String level) {
        return put("/config/" + pathSegment(subject), objectMapper.createObjectNode().put("compatibility", level))
                .thenApply(node -> requiredText(node, "compatibility"));
    }

    /**
     * Sets the registry's global compatibility level.
     *
     * @param level the level to set, for example {@code BACKWARD} or {@code FULL}
     * @return a future with the level the registry acknowledged
     */
    public CompletableFuture<String> setCompatibility(String level) {
        return put("/config", objectMapper.createObjectNode().put("compatibility", level))
                .thenApply(node -> requiredText(node, "compatibility"));
    }

    /**
     * Deletes a subject and all of its versions.
     *
     * @param subject the registry subject name
     * @param permanent whether to hard-delete instead of soft-delete
     * @return a future with the deleted version numbers
     */
    public CompletableFuture<List<Integer>> deleteSubject(String subject, boolean permanent) {
        return delete("/subjects/" + pathSegment(subject) + (permanent ? "?permanent=true" : ""))
                .thenApply(node -> stream(node).map(JsonNode::intValue).toList());
    }

    /**
     * Deletes one version of a subject.
     *
     * @param subject the registry subject name
     * @param version the version number to delete
     * @param permanent whether to hard-delete instead of soft-delete
     * @return a future with the deleted version number
     */
    public CompletableFuture<Integer> deleteVersion(String subject, int version, boolean permanent) {
        return delete("/subjects/" + pathSegment(subject) + "/versions/" + version
                        + (permanent ? "?permanent=true" : ""))
                .thenApply(JsonNode::intValue);
    }

    private ObjectNode payload(
            SchemaKind kind, String schema, String messageType, List<SchemaReference> references) {
        Objects.requireNonNull(kind, "kind");
        var body = objectMapper.createObjectNode().put("schema", Objects.requireNonNull(schema, "schema"));
        if (kind.wireName() != null) {
            body.put("schemaType", kind.wireName());
        }
        if (messageType != null) {
            body.put("messageType", messageType);
        }
        if (!references.isEmpty()) {
            var array = body.putArray("references");
            references.forEach(reference -> array.addObject()
                    .put("name", reference.name())
                    .put("subject", reference.subject())
                    .put("version", reference.version()));
        }
        return body;
    }

    private CompletableFuture<JsonNode> get(String path) {
        var request = request(path)
                .header("Accept", CONTENT_TYPE)
                .GET()
                .build();
        return send(request);
    }

    private CompletableFuture<JsonNode> post(String path, JsonNode body) {
        final String json;
        try {
            json = objectMapper.writeValueAsString(body);
        } catch (JsonProcessingException error) {
            return CompletableFuture.failedFuture(new SchemaRegistryException("cannot encode registry request", error));
        }
        var request = request(path)
                .header("Accept", CONTENT_TYPE)
                .header("Content-Type", CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        return send(request);
    }

    private CompletableFuture<JsonNode> put(String path, JsonNode body) {
        return sendWithBody(path, body, "PUT");
    }

    private CompletableFuture<JsonNode> delete(String path) {
        return send(request(path).header("Accept", CONTENT_TYPE).DELETE().build());
    }

    private CompletableFuture<JsonNode> sendWithBody(String path, JsonNode body, String method) {
        final String json;
        try {
            json = objectMapper.writeValueAsString(body);
        } catch (JsonProcessingException error) {
            return CompletableFuture.failedFuture(new SchemaRegistryException("cannot encode registry request", error));
        }
        return send(request(path)
                .header("Accept", CONTENT_TYPE)
                .header("Content-Type", CONTENT_TYPE)
                .method(method, HttpRequest.BodyPublishers.ofString(json))
                .build());
    }

    private HttpRequest.Builder request(String path) {
        var builder = HttpRequest.newBuilder(baseUri.resolve(path.replaceFirst("^/", "")));
        if (authorization != null) {
            builder.header("Authorization", authorization);
        }
        return builder;
    }

    private CompletableFuture<JsonNode> send(HttpRequest request) {
        return send(request, maxRetries);
    }

    private CompletableFuture<JsonNode> send(HttpRequest request, int retriesLeft) {
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .handle((response, error) -> {
                    if ((error != null || response.statusCode() == 429 || response.statusCode() >= 500)
                            && retriesLeft > 0) {
                        return send(request, retriesLeft - 1);
                    }
                    if (error != null) {
                        throw new CompletionException(new SchemaRegistryException("schema registry request failed", error));
                    }
                    if (response.statusCode() < 200 || response.statusCode() >= 300) {
                        throw new CompletionException(new SchemaRegistryException(response.statusCode(), response.body()));
                    }
                    try {
                        return CompletableFuture.completedFuture(objectMapper.readTree(response.body()));
                    } catch (JsonProcessingException parseError) {
                        throw new CompletionException(
                                new SchemaRegistryException("cannot parse schema registry response", parseError));
                    }
                }).thenCompose(future -> future);
    }

    private static RegisteredSchema registeredSchema(JsonNode node) {
        return new RegisteredSchema(
                requiredInt(node, "id"),
                node.path("version").asInt(),
                node.path("schema").asText(""),
                optionalText(node, "schemaType"),
                optionalText(node, "messageType"),
                references(node));
    }

    private static int requiredInt(JsonNode node, String name) {
        if (!node.has(name) || !node.get(name).canConvertToInt()) {
            throw new SchemaRegistryException("schema registry response has no integer " + name);
        }
        return node.get(name).intValue();
    }

    private static String requiredText(JsonNode node, String name) {
        var value = optionalText(node, name);
        if (value == null) {
            throw new SchemaRegistryException("schema registry response has no text " + name);
        }
        return value;
    }

    private static String optionalText(JsonNode node, String name) {
        var value = node.get(name);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static java.util.stream.Stream<JsonNode> stream(JsonNode node) {
        if (!node.isArray()) {
            throw new SchemaRegistryException("schema registry response is not an array");
        }
        return java.util.stream.StreamSupport.stream(node.spliterator(), false);
    }

    private static List<SchemaReference> references(JsonNode node) {
        var references = node.path("references");
        if (!references.isArray()) {
            return List.of();
        }
        return stream(references)
                .map(reference -> new SchemaReference(
                        requiredText(reference, "name"),
                        requiredText(reference, "subject"),
                        requiredInt(reference, "version")))
                .toList();
    }

    private static String pathSegment(String value) {
        return URLEncoder.encode(Objects.requireNonNull(value, "subject"), StandardCharsets.UTF_8)
                .replace("+", "%20");
    }

    /**
     * One registered version of a subject.
     *
     * @param id the schema's global registry ID
     * @param version the version number within the subject
     * @param schema the schema text
     * @param schemaType the registry schema type name, or null for Avro
     * @param messageType the Protobuf message full name, or null for other formats
     * @param references the schemas this schema refers to
     */
    public record RegisteredSchema(
            int id,
            int version,
            String schema,
            String schemaType,
            String messageType,
            List<SchemaReference> references) {
        /**
         * Copies the reference list so the record is immutable.
         *
         * @param id the schema's global registry ID
         * @param version the version number within the subject
         * @param schema the schema text
         * @param schemaType the registry schema type name, or null for Avro
         * @param messageType the Protobuf message full name, or null for other formats
         * @param references the schemas this schema refers to
         */
        public RegisteredSchema {
            references = List.copyOf(references);
        }

        /**
         * Creates a version without references.
         *
         * @param id the schema's global registry ID
         * @param version the version number within the subject
         * @param schema the schema text
         * @param schemaType the registry schema type name, or null for Avro
         * @param messageType the Protobuf message full name, or null for other formats
         */
        public RegisteredSchema(int id, int version, String schema, String schemaType, String messageType) {
            this(id, version, schema, schemaType, messageType, List.of());
        }
    }

    /**
     * A schema fetched by ID, with unresolved references.
     *
     * @param schema the schema text
     * @param messageType the Protobuf message full name, or null for other formats
     * @param references the schemas this schema refers to, by name and subject version
     */
    public record FetchedSchema(String schema, String messageType, List<SchemaReference> references) {
        /**
         * Copies the reference list so the record is immutable.
         *
         * @param schema the schema text
         * @param messageType the Protobuf message full name, or null for other formats
         * @param references the schemas this schema refers to
         */
        public FetchedSchema {
            references = List.copyOf(references);
        }

        /**
         * Creates a fetched schema without references.
         *
         * @param schema the schema text
         * @param messageType the Protobuf message full name, or null for other formats
         */
        public FetchedSchema(String schema, String messageType) {
            this(schema, messageType, List.of());
        }
    }

    /**
     * A reference from one schema to a version of another subject.
     *
     * @param name the name the referencing schema uses, for example an Avro full name
     *     or a Protobuf import path
     * @param subject the referenced subject
     * @param version the referenced version within that subject
     */
    public record SchemaReference(String name, String subject, int version) {
    }

    /**
     * A schema fetched by ID with every transitive reference resolved to its text.
     *
     * @param schema the schema text
     * @param messageType the Protobuf message full name, or null for other formats
     * @param references reference name to schema text, in dependency-first order
     */
    public record ResolvedSchema(String schema, String messageType, java.util.Map<String, String> references) {
        /**
         * Copies the reference map so the record is immutable.
         *
         * @param schema the schema text
         * @param messageType the Protobuf message full name, or null for other formats
         * @param references reference name to schema text
         */
        public ResolvedSchema {
            references = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(references));
        }
    }
}
