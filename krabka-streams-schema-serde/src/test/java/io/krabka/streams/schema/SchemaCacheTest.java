package io.krabka.streams.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class SchemaCacheTest {
    @Test
    void autoRegisterUsesVersionsEndpoint() throws Exception {
        try (var server = new RegistryStub()) {
            server.reply("POST", "/subjects/orders-value/versions", 200, "{\"id\":50}");
            var cache = new SchemaCache(new KrabkaSchemaRegistryClient(server.uri()));
            cache.intern("orders-value", SchemaKind.AVRO, "\"string\"", null);

            cache.prewarm().join();

            assertEquals(50, cache.idForSubject("orders-value").orElseThrow());
            assertEquals(1, server.count("POST", "/subjects/orders-value/versions"));
        }
    }

    @Test
    void lookupOnlyUsesSubjectEndpoint() throws Exception {
        try (var server = new RegistryStub()) {
            server.reply("POST", "/subjects/orders-value", 200, "{\"id\":51}");
            var cache = new SchemaCache(
                    new KrabkaSchemaRegistryClient(server.uri()),
                    RegisterMode.LOOKUP_ONLY,
                    new TopicNameStrategy());
            cache.intern("orders-value", SchemaKind.JSON, "{\"type\":\"object\"}", null);

            cache.prewarm().join();

            assertEquals(51, cache.idForSubject("orders-value").orElseThrow());
            assertEquals(1, server.count("POST", "/subjects/orders-value"));
        }
    }

    @Test
    void useLatestKeepsRegistryMessageType() throws Exception {
        try (var server = new RegistryStub()) {
            server.reply(
                    "GET",
                    "/subjects/orders-value/versions/latest",
                    200,
                    "{\"id\":52,\"messageType\":\"demo.Latest\"}");
            var cache = new SchemaCache(
                    new KrabkaSchemaRegistryClient(server.uri()),
                    RegisterMode.USE_LATEST,
                    new TopicNameStrategy());
            cache.intern("orders-value", SchemaKind.PROTOBUF, "syntax = \"proto3\";", "demo.Local");

            cache.prewarm().join();

            assertEquals(52, cache.idForSubject("orders-value").orElseThrow());
            assertEquals("demo.Latest", cache.writerMessageType(52));
        }
    }

    @Test
    void unknownWriterSchemaStartsOneBackgroundFetch() throws Exception {
        try (var server = new RegistryStub()) {
            server.reply(
                    "GET",
                    "/schemas/ids/7",
                    200,
                    "{\"schema\":\"\\\"string\\\"\",\"messageType\":\"demo.Value\"}");
            var cache = new SchemaCache(new KrabkaSchemaRegistryClient(server.uri()));

            assertThrows(SchemaFetchPendingException.class, () -> cache.writerSchema(7));
            assertThrows(SchemaFetchPendingException.class, () -> cache.writerSchema(7));

            org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
                while (true) {
                    try {
                        assertEquals("\"string\"", cache.writerSchema(7));
                        break;
                    } catch (SchemaFetchPendingException pending) {
                        Thread.sleep(10);
                    }
                }
            });
            assertEquals("demo.Value", cache.writerMessageType(7));
            assertEquals(1, server.count("GET", "/schemas/ids/7"));
        }
    }
}
