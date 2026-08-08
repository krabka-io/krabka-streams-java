package io.krabka.streams.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.krabka.streams.schema.KrabkaSchemaRegistryClient;
import io.krabka.streams.schema.SchemaKind;
import org.junit.jupiter.api.Test;

class SchemaRegistryStubTest {
    @Test
    void supportsRegisterLookupLatestAndFetch() throws Exception {
        try (var stub = new SchemaRegistryStub()) {
            var client = new KrabkaSchemaRegistryClient(stub.uri());

            int id = client.register("orders-value", SchemaKind.JSON, "{\"type\":\"string\"}", null).join();

            assertEquals(id, client.lookup(
                            "orders-value", SchemaKind.JSON, "{\"type\":\"string\"}", null)
                    .join());
            assertEquals(id, client.latestId("orders-value").join());
            assertEquals("{\"type\":\"string\"}", client.schemaById(id).join().schema());
            assertEquals(1, stub.requestCount("POST", "/subjects/orders-value/versions"));
        }
    }
}
