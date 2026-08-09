package io.krabka.streams.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class KrabkaSchemaRegistryClientTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void registersAvroWithoutSchemaType() throws Exception {
        try (var server = new RegistryStub()) {
            server.reply("POST", "/subjects/orders-value/versions", 200, "{\"id\":42}");
            var client = new KrabkaSchemaRegistryClient(server.uri());

            assertEquals(42, client.register("orders-value", SchemaKind.AVRO, "\"string\"", null).join());

            var body = JSON.readTree(server.body("POST", "/subjects/orders-value/versions"));
            assertEquals("\"string\"", body.get("schema").asText());
            assertNull(body.get("schemaType"));
        }
    }

    @Test
    void sendsProtobufMetadataAndReadsLatest() throws Exception {
        try (var server = new RegistryStub()) {
            server.reply("POST", "/subjects/orders-value/versions", 200, "{\"id\":43}");
            server.reply(
                    "GET",
                    "/subjects/orders-value/versions/latest",
                    200,
                    "{\"id\":43,\"version\":2,\"schema\":\"syntax = \\\"proto3\\\";\","
                            + "\"schemaType\":\"PROTOBUF\",\"messageType\":\"demo.Order\"}");
            var client = new KrabkaSchemaRegistryClient(server.uri());

            client.register("orders-value", SchemaKind.PROTOBUF, "syntax = \"proto3\";", "demo.Order").join();
            var latest = client.latest("orders-value").join();

            var body = JSON.readTree(server.body("POST", "/subjects/orders-value/versions"));
            assertEquals("PROTOBUF", body.get("schemaType").asText());
            assertEquals("demo.Order", body.get("messageType").asText());
            assertEquals(43, latest.id());
            assertEquals("demo.Order", latest.messageType());
        }
    }

    @Test
    void reportsRegistryStatusAndBody() throws Exception {
        try (var server = new RegistryStub()) {
            server.reply("GET", "/schemas/ids/7", 404, "missing schema");
            var client = new KrabkaSchemaRegistryClient(server.uri());

            var failure = org.junit.jupiter.api.Assertions.assertThrows(
                    CompletionException.class, () -> client.schemaById(7).join());
            var registryFailure = assertInstanceOf(SchemaRegistryException.class, failure.getCause());

            assertEquals(404, registryFailure.statusCode());
            assertTrue(registryFailure.getMessage().contains("missing schema"));
        }
    }

    @Test
    void preservesContextPathAndSupportsRegistryManagement() throws Exception {
        try (var server = new RegistryStub()) {
            server.reply("GET", "/registry/subjects", 200, "[\"orders-value\"]");
            server.reply("GET", "/registry/subjects/orders-value/versions", 200, "[1,2]");
            server.reply("GET", "/registry/config/orders-value", 200, "{\"compatibilityLevel\":\"BACKWARD\"}");
            server.reply("PUT", "/registry/config/orders-value", 200, "{\"compatibility\":\"FULL\"}");
            server.reply("DELETE", "/registry/subjects/orders-value", 200, "[1,2]");
            var client = new KrabkaSchemaRegistryClient(server.uri().resolve("/registry"));

            assertEquals(java.util.List.of("orders-value"), client.subjects().join());
            assertEquals(java.util.List.of(1, 2), client.versions("orders-value").join());
            assertEquals("BACKWARD", client.compatibility("orders-value").join());
            assertEquals("FULL", client.setCompatibility("orders-value", "FULL").join());
            assertEquals(java.util.List.of(1, 2), client.deleteSubject("orders-value", true).join());
        }
    }
}
