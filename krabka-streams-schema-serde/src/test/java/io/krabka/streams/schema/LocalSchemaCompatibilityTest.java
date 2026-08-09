package io.krabka.streams.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.testing.junit.testparameterinjector.junit5.TestParameter;
import com.google.testing.junit.testparameterinjector.junit5.TestParameterInjectorTest;

class LocalSchemaCompatibilityTest {
    @TestParameterInjectorTest
    void detectsAvroTypeChanges(@TestParameter LocalSchemaCompatibility.Mode mode) {
        var previous = "{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
        var candidate = "{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";

        assertThat(LocalSchemaCompatibility.avro(previous, candidate, mode).compatible()).isFalse();
    }

    @TestParameterInjectorTest
    void detectsJsonTypeChanges(@TestParameter LocalSchemaCompatibility.Mode mode) {
        var previous = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}}}";
        var candidate = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}}}";

        assertThat(LocalSchemaCompatibility.json(previous, candidate, mode).compatible()).isFalse();
    }

    @TestParameterInjectorTest
    void detectsProtobufWireTypeChanges(@TestParameter LocalSchemaCompatibility.Mode mode)
            throws Descriptors.DescriptorValidationException {
        var previous = descriptor(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64, false);
        var candidate = descriptor(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false);

        assertThat(LocalSchemaCompatibility.protobuf(previous, candidate, mode).compatible()).isFalse();
    }

    @TestParameterInjectorTest
    void acceptsCompatibleEvolution(@TestParameter LocalSchemaCompatibility.Mode mode)
            throws Descriptors.DescriptorValidationException {
        var expected = new LocalSchemaCompatibility.Result(true, java.util.List.of());
        var previousAvro = "{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
        var candidateAvro = "{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},"
                + "{\"name\":\"note\",\"type\":[\"null\",\"string\"],\"default\":null}]}";
        var previousJson = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}}}";
        var candidateJson = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"note\":{\"type\":\"string\"}}}";

        assertThat(LocalSchemaCompatibility.avro(previousAvro, candidateAvro, mode))
                .usingRecursiveComparison()
                .isEqualTo(expected);
        assertThat(LocalSchemaCompatibility.json(previousJson, candidateJson, mode))
                .usingRecursiveComparison()
                .isEqualTo(expected);
        assertThat(LocalSchemaCompatibility.protobuf(
                        descriptor(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64, false),
                        descriptor(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64, true),
                        mode))
                .usingRecursiveComparison()
                .isEqualTo(expected);
    }

    @org.junit.jupiter.api.Test
    void detectsNarrowingFromAnUnconstrainedJsonSchema() {
        var actual = LocalSchemaCompatibility.json(
                "{}", "{\"type\":\"string\"}", LocalSchemaCompatibility.Mode.BACKWARD);

        assertThat(actual.compatible()).isFalse();
    }

    private static Descriptors.FileDescriptor descriptor(
            DescriptorProtos.FieldDescriptorProto.Type type, boolean extraField)
            throws Descriptors.DescriptorValidationException {
        var message = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Value")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("id")
                        .setNumber(1)
                        .setType(type));
        if (extraField) {
            message.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("note")
                    .setNumber(2)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        }
        var file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("value.proto")
                .setPackage("demo")
                .setSyntax("proto3")
                .addMessageType(message)
                .build();
        return Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0]);
    }
}
