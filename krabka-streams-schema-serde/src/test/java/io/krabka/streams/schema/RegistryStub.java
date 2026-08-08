package io.krabka.streams.schema;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class RegistryStub implements AutoCloseable {
    private final HttpServer server;
    private final Map<String, Reply> replies = new ConcurrentHashMap<>();
    private final Map<String, String> bodies = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

    RegistryStub() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    URI uri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    void reply(String method, String path, int status, String body) {
        replies.put(method + " " + path, new Reply(status, body));
    }

    String body(String method, String path) {
        return bodies.get(method + " " + path);
    }

    int count(String method, String path) {
        var count = counts.get(method + " " + path);
        return count == null ? 0 : count.get();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        var key = exchange.getRequestMethod() + " " + exchange.getRequestURI().getRawPath();
        bodies.put(key, new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        counts.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
        var reply = replies.getOrDefault(key, new Reply(404, "not found"));
        var bytes = reply.body().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(reply.status(), bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private record Reply(int status, String body) {
    }
}
