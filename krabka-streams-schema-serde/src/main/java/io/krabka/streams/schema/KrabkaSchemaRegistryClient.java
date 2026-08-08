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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** A nonblocking client for the Confluent Schema Registry REST API. */
public final class KrabkaSchemaRegistryClient {
    private static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    private final URI baseUri;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public KrabkaSchemaRegistryClient(URI baseUri) {
        this(baseUri, HttpClient.newHttpClient(), new ObjectMapper());
    }

    public KrabkaSchemaRegistryClient(URI baseUri, HttpClient httpClient, ObjectMapper objectMapper) {
        var normalized = Objects.requireNonNull(baseUri, "baseUri").toString().replaceAll("/+$", "");
        this.baseUri = URI.create(normalized);
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    public CompletableFuture<Integer> register(
            String subject, SchemaKind kind, String schema, String messageType) {
        return post("/subjects/" + pathSegment(subject) + "/versions", payload(kind, schema, messageType))
                .thenApply(response -> requiredInt(response, "id"));
    }

    public CompletableFuture<Integer> lookup(
            String subject, SchemaKind kind, String schema, String messageType) {
        return post("/subjects/" + pathSegment(subject), payload(kind, schema, messageType))
                .thenApply(response -> requiredInt(response, "id"));
    }

    public CompletableFuture<RegisteredSchema> latest(String subject) {
        return get("/subjects/" + pathSegment(subject) + "/versions/latest")
                .thenApply(KrabkaSchemaRegistryClient::registeredSchema);
    }

    public CompletableFuture<Integer> latestId(String subject) {
        return latest(subject).thenApply(RegisteredSchema::id);
    }

    public CompletableFuture<FetchedSchema> schemaById(int schemaId) {
        return get("/schemas/ids/" + Integer.toUnsignedString(schemaId))
                .thenApply(node -> new FetchedSchema(requiredText(node, "schema"), optionalText(node, "messageType")));
    }

    private ObjectNode payload(SchemaKind kind, String schema, String messageType) {
        Objects.requireNonNull(kind, "kind");
        var body = objectMapper.createObjectNode().put("schema", Objects.requireNonNull(schema, "schema"));
        if (kind.wireName() != null) {
            body.put("schemaType", kind.wireName());
        }
        if (messageType != null) {
            body.put("messageType", messageType);
        }
        return body;
    }

    private CompletableFuture<JsonNode> get(String path) {
        var request = HttpRequest.newBuilder(baseUri.resolve(path))
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
        var request = HttpRequest.newBuilder(baseUri.resolve(path))
                .header("Accept", CONTENT_TYPE)
                .header("Content-Type", CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        return send(request);
    }

    private CompletableFuture<JsonNode> send(HttpRequest request) {
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .handle((response, error) -> {
                    if (error != null) {
                        throw new CompletionException(new SchemaRegistryException("schema registry request failed", error));
                    }
                    if (response.statusCode() < 200 || response.statusCode() >= 300) {
                        throw new CompletionException(new SchemaRegistryException(response.statusCode(), response.body()));
                    }
                    try {
                        return objectMapper.readTree(response.body());
                    } catch (JsonProcessingException parseError) {
                        throw new CompletionException(
                                new SchemaRegistryException("cannot parse schema registry response", parseError));
                    }
                });
    }

    private static RegisteredSchema registeredSchema(JsonNode node) {
        return new RegisteredSchema(
                requiredInt(node, "id"),
                node.path("version").asInt(),
                node.path("schema").asText(""),
                optionalText(node, "schemaType"),
                optionalText(node, "messageType"));
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

    private static String pathSegment(String value) {
        return URLEncoder.encode(Objects.requireNonNull(value, "subject"), StandardCharsets.UTF_8)
                .replace("+", "%20");
    }

    public record RegisteredSchema(int id, int version, String schema, String schemaType, String messageType) {
    }

    public record FetchedSchema(String schema, String messageType) {
    }
}
