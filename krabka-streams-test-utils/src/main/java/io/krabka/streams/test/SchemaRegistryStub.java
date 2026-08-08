package io.krabka.streams.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** A small, stateful Confluent Schema Registry test server. */
public final class SchemaRegistryStub implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpServer server;
    private final AtomicInteger nextId = new AtomicInteger(1);
    private final Map<SchemaKey, Integer> idsBySchema = new LinkedHashMap<>();
    private final Map<Integer, SchemaValue> schemasById = new HashMap<>();
    private final Map<String, List<Integer>> subjectVersions = new HashMap<>();
    private final Map<RequestKey, AtomicInteger> requestCounts = new HashMap<>();

    public SchemaRegistryStub() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    public URI uri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    public synchronized int requestCount(String method, String path) {
        var count = requestCounts.get(new RequestKey(method, path));
        return count == null ? 0 : count.get();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private synchronized void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getRawPath();
        requestCounts.computeIfAbsent(new RequestKey(method, path), ignored -> new AtomicInteger())
                .incrementAndGet();
        try {
            if ("POST".equals(method) && path.endsWith("/versions")) {
                register(exchange, subject(path, "/versions"));
            } else if ("POST".equals(method) && path.startsWith("/subjects/")) {
                lookup(exchange, subject(path, ""));
            } else if ("GET".equals(method) && path.endsWith("/versions/latest")) {
                latest(exchange, subject(path, "/versions/latest"));
            } else if ("GET".equals(method) && path.startsWith("/schemas/ids/")) {
                byId(exchange, parseId(path));
            } else {
                reply(exchange, 404, error(40401, "subject or schema not found"));
            }
        } catch (IllegalArgumentException error) {
            reply(exchange, 422, error(42201, error.getMessage()));
        }
    }

    private void register(HttpExchange exchange, String subject) throws IOException {
        var value = readSchema(exchange);
        var key = value.key();
        int id = idsBySchema.computeIfAbsent(key, ignored -> nextId.getAndIncrement());
        schemasById.putIfAbsent(id, value);
        subjectVersions.computeIfAbsent(subject, ignored -> new ArrayList<>());
        var versions = subjectVersions.get(subject);
        if (!versions.contains(id)) {
            versions.add(id);
        }
        reply(exchange, 200, JSON.createObjectNode().put("id", id));
    }

    private void lookup(HttpExchange exchange, String subject) throws IOException {
        var value = readSchema(exchange);
        Integer id = idsBySchema.get(value.key());
        var versions = subjectVersions.get(subject);
        if (id == null || versions == null || !versions.contains(id)) {
            reply(exchange, 404, error(40403, "schema not found"));
            return;
        }
        var body = schemaBody(id, value).put("subject", subject).put("version", versions.indexOf(id) + 1);
        reply(exchange, 200, body);
    }

    private void latest(HttpExchange exchange, String subject) throws IOException {
        var versions = subjectVersions.get(subject);
        if (versions == null || versions.isEmpty()) {
            reply(exchange, 404, error(40401, "subject not found"));
            return;
        }
        int id = versions.get(versions.size() - 1);
        var body = schemaBody(id, schemasById.get(id))
                .put("subject", subject)
                .put("version", versions.size());
        reply(exchange, 200, body);
    }

    private void byId(HttpExchange exchange, int id) throws IOException {
        var value = schemasById.get(id);
        if (value == null) {
            reply(exchange, 404, error(40403, "schema not found"));
            return;
        }
        reply(exchange, 200, schemaBody(id, value));
    }

    private static SchemaValue readSchema(HttpExchange exchange) throws IOException {
        JsonNode body = JSON.readTree(exchange.getRequestBody());
        var schema = requiredText(body, "schema");
        var type = optionalText(body, "schemaType");
        var messageType = optionalText(body, "messageType");
        return new SchemaValue(schema, type, messageType);
    }

    private static ObjectNode schemaBody(int id, SchemaValue value) {
        var body = JSON.createObjectNode().put("id", id).put("schema", value.schema());
        if (value.schemaType() != null) {
            body.put("schemaType", value.schemaType());
        }
        if (value.messageType() != null) {
            body.put("messageType", value.messageType());
        }
        return body;
    }

    private static ObjectNode error(int code, String message) {
        return JSON.createObjectNode().put("error_code", code).put("message", message);
    }

    private static String subject(String path, String suffix) {
        String encoded = path.substring("/subjects/".length(), path.length() - suffix.length());
        return URLDecoder.decode(encoded, StandardCharsets.UTF_8);
    }

    private static int parseId(String path) {
        return Integer.parseInt(path.substring("/schemas/ids/".length()));
    }

    private static String requiredText(JsonNode node, String name) {
        var value = optionalText(node, name);
        if (value == null) {
            throw new IllegalArgumentException("request has no text " + name);
        }
        return value;
    }

    private static String optionalText(JsonNode node, String name) {
        var value = node.get(name);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static void reply(HttpExchange exchange, int status, JsonNode body) throws IOException {
        var bytes = JSON.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/vnd.schemaregistry.v1+json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private record RequestKey(String method, String path) {
        private RequestKey {
            Objects.requireNonNull(method, "method");
            Objects.requireNonNull(path, "path");
        }
    }

    private record SchemaKey(String schema, String schemaType, String messageType) {
    }

    private record SchemaValue(String schema, String schemaType, String messageType) {
        private SchemaKey key() {
            return new SchemaKey(schema, schemaType, messageType);
        }
    }
}
