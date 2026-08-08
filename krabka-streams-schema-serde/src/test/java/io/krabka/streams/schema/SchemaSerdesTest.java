package io.krabka.streams.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.StringValue;
import java.net.URI;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

class SchemaSerdesTest {
    private static SchemaCache offlineCache() {
        return new SchemaCache(new KrabkaSchemaRegistryClient(URI.create("http://127.0.0.1:1")));
    }

    @Test
    void avroRoundTripsWithWriterResolution() {
        var schema = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");
        var cache = offlineCache();
        cache.seedSubjectId("orders-value", 11);
        cache.seedWriterSchema(11, schema.toString());
        var serde = AvroSerde.generic(schema, cache, Role.VALUE);
        var order = new GenericData.Record(schema);
        order.put("id", "o-1");

        byte[] bytes = serde.serializer().serialize("orders", order);
        GenericRecord decoded = serde.deserializer().deserialize("orders", bytes);

        assertEquals("o-1", decoded.get("id").toString());
    }

    @Test
    void protobufRoundTripsAndChecksMessageType() {
        var cache = offlineCache();
        cache.seedSubjectId("messages-value", 12);
        cache.seedWriterSchema(12, "syntax = \"proto3\";");
        cache.seedWriterMessageType(12, StringValue.getDescriptor().getFullName());
        var serde = ProtobufSerde.forValue(StringValue.getDefaultInstance(), cache);

        var bytes = serde.serializer().serialize("messages", StringValue.of("hello"));
        var decoded = serde.deserializer().deserialize("messages", bytes);

        assertEquals("hello", decoded.getValue());

        cache.seedWriterMessageType(12, "demo.Other");
        assertThrows(SerializationException.class, () -> serde.deserializer().deserialize("messages", bytes));
    }

    @Test
    void jsonRoundTripsAndValidatesWriterSchema() {
        var schema = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},"
                + "\"required\":[\"id\"]}";
        var cache = offlineCache();
        cache.seedSubjectId("orders-value", 13);
        cache.seedWriterSchema(13, schema);
        var serde = JsonSchemaSerde.forValue(Order.class, schema, cache, true);

        var bytes = serde.serializer().serialize("orders", new Order("o-1"));
        var decoded = serde.deserializer().deserialize("orders", bytes);

        assertEquals(new Order("o-1"), decoded);

        var invalid = ConfluentWireFormat.encode(13, "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThrows(SerializationException.class, () -> serde.deserializer().deserialize("orders", invalid));
    }

    @Test
    void kafkaNullsRemainNull() {
        var schema = "{\"type\":\"object\"}";
        var serde = JsonSchemaSerde.forValue(Order.class, schema, offlineCache(), false);

        assertNull(serde.serializer().serialize("orders", null));
        assertNull(serde.deserializer().deserialize("orders", null));
    }

    record Order(String id) {
    }
}
