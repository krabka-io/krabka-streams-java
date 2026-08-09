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
        assertThrows(SerializationException.class, () -> serde.serializer().serialize("orders", new Order(null)));
    }

    @Test
    void kafkaNullsRemainNull() {
        var schema = "{\"type\":\"object\"}";
        var serde = JsonSchemaSerde.forValue(Order.class, schema, offlineCache(), false);

        assertNull(serde.serializer().serialize("orders", null));
        assertNull(serde.deserializer().deserialize("orders", null));
    }

    @Test
    void serdeCanOverrideCacheSubjectStrategy() {
        var schema = "{\"type\":\"object\"}";
        var cache = offlineCache();
        cache.seedSubjectId("record.Order", 14);
        var serde = JsonSchemaSerde.forValue(
                Order.class,
                schema,
                cache,
                false,
                com.networknt.schema.SpecificationVersion.DRAFT_7,
                (topic, role) -> "record.Order",
                new com.fasterxml.jackson.databind.ObjectMapper());

        assertEquals(14, ConfluentWireFormat.decode(
                serde.serializer().serialize("ignored", new Order("o-2"))).schemaId());
    }

    @Test
    void avroReflectionRoundTripsPojo() {
        var cache = offlineCache();
        var serde = AvroSerde.reflect(ReflectedOrder.class, cache, Role.VALUE);
        cache.seedSubjectId("orders-value", 15);
        var schema = org.apache.avro.reflect.ReflectData.get().getSchema(ReflectedOrder.class);
        cache.seedWriterSchema(15, schema.toString());
        var value = new ReflectedOrder();
        value.id = "o-3";

        var decoded = serde.deserializer().deserialize(
                "orders", serde.serializer().serialize("orders", value));

        assertEquals("o-3", decoded.id);
    }

    @Test
    void protobufPrintsFullDescriptorsAndFramesNestedIndexes() throws Exception {
        var nested = com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Nested")
                .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("value")
                        .setNumber(1)
                        .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        var outer = com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Outer")
                .addNestedType(nested)
                .addOneofDecl(com.google.protobuf.DescriptorProtos.OneofDescriptorProto.newBuilder().setName("choice"))
                .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("name")
                        .setNumber(1)
                        .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                        .setOneofIndex(0))
                .build();
        var file = com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("demo.proto")
                .setSyntax("proto3")
                .setPackage("demo")
                .setOptions(com.google.protobuf.DescriptorProtos.FileOptions.newBuilder()
                        .setJavaPackage("demo.generated"))
                .addEnumType(com.google.protobuf.DescriptorProtos.EnumDescriptorProto.newBuilder()
                        .setName("Status")
                        .addValue(com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                                .setName("UNKNOWN")
                                .setNumber(0)))
                .addMessageType(outer)
                .addService(com.google.protobuf.DescriptorProtos.ServiceDescriptorProto.newBuilder()
                        .setName("Demo")
                        .addMethod(com.google.protobuf.DescriptorProtos.MethodDescriptorProto.newBuilder()
                                .setName("Get")
                                .setInputType(".demo.Outer")
                                .setOutputType(".demo.Outer")))
                .build();
        var descriptor = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(file, new com.google.protobuf.Descriptors.FileDescriptor[0]);
        var printed = ProtobufSchemaPrinter.print(descriptor);
        assertEquals(true, printed.contains("option java_package = \"demo.generated\";"));
        assertEquals(true, printed.contains("message Nested"));
        assertEquals(true, printed.contains("enum Status"));
        assertEquals(true, printed.contains("oneof choice"));
        assertEquals(true, printed.contains("service Demo"));

        var nestedDescriptor = descriptor.findMessageTypeByName("Outer").findNestedTypeByName("Nested");
        var defaultInstance = com.google.protobuf.DynamicMessage.getDefaultInstance(nestedDescriptor);
        var cache = offlineCache();
        cache.seedSubjectId("messages-value", 16);
        var serde = ProtobufSerde.forValue(defaultInstance, cache);
        var frame = ConfluentWireFormat.decodeProtobuf(
                serde.serializer().serialize("messages", defaultInstance));
        assertEquals(java.util.List.of(0, 0), frame.messageIndexes());
    }

    record Order(String id) {
    }

    public static final class ReflectedOrder {
        public String id;

        public ReflectedOrder() {
        }
    }
}
