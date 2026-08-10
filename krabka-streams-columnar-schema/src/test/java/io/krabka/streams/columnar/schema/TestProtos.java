package io.krabka.streams.columnar.schema;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.StructProto;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.WrappersProto;

/**
 * Builds a dynamic test message covering every mapped Protobuf shape, because this
 * repository has no protoc code generation.
 */
final class TestProtos {
    private static final Descriptors.FileDescriptor FILE = build();

    private TestProtos() {
    }

    static Descriptors.Descriptor everything() {
        return FILE.findMessageTypeByName("Everything");
    }

    static Descriptors.Descriptor child() {
        return FILE.findMessageTypeByName("Child");
    }

    private static Descriptors.FileDescriptor build() {
        var color = DescriptorProtos.EnumDescriptorProto.newBuilder()
                .setName("Color")
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(0))
                .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("BLUE").setNumber(1));
        var child = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Child")
                .addField(scalar("name", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(message("next", 2, ".krabka.test.Child"));
        var labelsEntry = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("LabelsEntry")
                .setOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true))
                .addField(scalar("key", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(scalar("value", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64));
        var everything = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Everything")
                .addNestedType(labelsEntry)
                .addField(scalar("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(scalar("count", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32))
                .addField(scalar("ucount", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32))
                .addField(scalar("big_count", 4, DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64))
                .addField(scalar("ratio", 5, DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE))
                .addField(scalar("flag", 6, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL))
                .addField(scalar("payload", 7, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES))
                .addField(enumField("color", 8, ".krabka.test.Color"))
                .addField(message("chld", 9, ".krabka.test.Child"))
                .addField(repeated("tags", 10, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(map("labels", 11, ".krabka.test.Everything.LabelsEntry"))
                .addField(message("stamp", 12, ".google.protobuf.Timestamp"))
                .addField(message("maybe_name", 13, ".google.protobuf.StringValue"))
                .addField(message("meta", 14, ".google.protobuf.Struct"))
                .addField(oneof("either_text", 15, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(oneof("either_num", 16, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                .addOneofDecl(DescriptorProtos.OneofDescriptorProto.newBuilder().setName("either"));
        var file = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("krabka_test.proto")
                .setPackage("krabka.test")
                .setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addDependency("google/protobuf/wrappers.proto")
                .addDependency("google/protobuf/struct.proto")
                .addEnumType(color)
                .addMessageType(child)
                .addMessageType(everything)
                .build();
        try {
            return Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[] {
                TimestampProto.getDescriptor(), WrappersProto.getDescriptor(), StructProto.getDescriptor(),
            });
        } catch (Descriptors.DescriptorValidationException error) {
            throw new AssertionError(error);
        }
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder scalar(
            String name, int number, DescriptorProtos.FieldDescriptorProto.Type type) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder repeated(
            String name, int number, DescriptorProtos.FieldDescriptorProto.Type type) {
        return scalar(name, number, type)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder message(
            String name, int number, String typeName) {
        return scalar(name, number, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(typeName);
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder map(
            String name, int number, String entryTypeName) {
        return message(name, number, entryTypeName)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder enumField(
            String name, int number, String typeName) {
        return scalar(name, number, DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
                .setTypeName(typeName);
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder oneof(
            String name, int number, DescriptorProtos.FieldDescriptorProto.Type type) {
        return scalar(name, number, type).setOneofIndex(0);
    }
}
