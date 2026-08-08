package io.krabka.streams.schema;

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;

final class ProtobufSchemaPrinter {
    private ProtobufSchemaPrinter() {
    }

    static String print(FileDescriptor file) {
        var output = new StringBuilder("syntax = \"")
                .append(isProto2(file) ? "proto2" : "proto3")
                .append("\";\n");
        if (!file.getPackage().isEmpty()) {
            output.append("package ").append(file.getPackage()).append(";\n");
        }
        for (var message : file.getMessageTypes()) {
            appendMessage(output, message);
        }
        return output.toString();
    }

    private static void appendMessage(StringBuilder output, Descriptor message) {
        output.append("\nmessage ").append(message.getName()).append(" {\n");
        for (var field : message.getFields()) {
            output.append("  ");
            if (field.isRepeated() && !field.isMapField()) {
                output.append("repeated ");
            } else if (isProto2(field.getFile()) && field.isRequired()) {
                output.append("required ");
            } else if (isProto2(field.getFile())) {
                output.append("optional ");
            }
            output.append(typeName(field))
                    .append(' ')
                    .append(field.getName())
                    .append(" = ")
                    .append(field.getNumber())
                    .append(";\n");
        }
        output.append("}\n");
    }

    private static String typeName(FieldDescriptor field) {
        if (field.isMapField()) {
            var key = field.getMessageType().findFieldByNumber(1);
            var value = field.getMessageType().findFieldByNumber(2);
            return "map<" + typeName(key) + ", " + typeName(value) + ">";
        }
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            return field.getMessageType().getFullName();
        }
        if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
            return field.getEnumType().getFullName();
        }
        Type type = field.toProto().getType();
        return switch (type) {
            case TYPE_DOUBLE -> "double";
            case TYPE_FLOAT -> "float";
            case TYPE_INT64 -> "int64";
            case TYPE_UINT64 -> "uint64";
            case TYPE_INT32 -> "int32";
            case TYPE_FIXED64 -> "fixed64";
            case TYPE_FIXED32 -> "fixed32";
            case TYPE_BOOL -> "bool";
            case TYPE_STRING -> "string";
            case TYPE_BYTES -> "bytes";
            case TYPE_UINT32 -> "uint32";
            case TYPE_SFIXED32 -> "sfixed32";
            case TYPE_SFIXED64 -> "sfixed64";
            case TYPE_SINT32 -> "sint32";
            case TYPE_SINT64 -> "sint64";
            default -> throw new IllegalArgumentException("unsupported Protobuf field type " + type);
        };
    }

    private static boolean isProto2(FileDescriptor file) {
        return "proto2".equals(file.toProto().getSyntax());
    }
}
