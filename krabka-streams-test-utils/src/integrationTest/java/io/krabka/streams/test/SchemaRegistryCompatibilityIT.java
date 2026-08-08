package io.krabka.streams.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.krabka.streams.schema.AvroSerde;
import io.krabka.streams.schema.KrabkaSchemaRegistryClient;
import io.krabka.streams.schema.Role;
import io.krabka.streams.schema.SchemaCache;
import io.krabka.streams.schema.SchemaKind;
import java.net.URI;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "KRABKA_INTEGRATION_SCHEMA_REGISTRY", matches = ".+")
class SchemaRegistryCompatibilityIT {
    @Test
    void registersLooksUpFetchesAndRoundTripsAvro() {
        var uri = URI.create(System.getenv("KRABKA_INTEGRATION_SCHEMA_REGISTRY"));
        var client = new KrabkaSchemaRegistryClient(uri);
        var cache = new SchemaCache(client);
        var schema = new Schema.Parser().parse("""
                {"type":"record","name":"Event","namespace":"io.krabka.test",
                 "fields":[{"name":"name","type":"string"}]}
                """);
        String topic = "registry-it-" + UUID.randomUUID().toString().replace("-", "");
        String subject = topic + "-value";
        var serde = AvroSerde.generic(schema, cache, Role.VALUE);
        String canonicalSchema = SchemaNormalization.toParsingForm(schema);
        serde.registerSubject(topic);
        cache.prewarm().join();

        int id = cache.idForSubject(subject).orElseThrow();
        var value = new GenericData.Record(schema);
        value.put("name", "krabka");
        byte[] encoded = serde.serializer().serialize(topic, value);
        var decoded = serde.deserializer().deserialize(topic, encoded);

        assertEquals("krabka", decoded.get("name").toString());
        assertEquals(id, client.lookup(subject, SchemaKind.AVRO, canonicalSchema, null).join());
        assertEquals(id, client.latestId(subject).join());
        assertEquals(canonicalSchema, client.schemaById(id).join().schema());
    }
}
