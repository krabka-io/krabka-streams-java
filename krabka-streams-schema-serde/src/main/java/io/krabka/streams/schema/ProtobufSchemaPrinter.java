package io.krabka.streams.schema;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.GenericDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;

/** Reconstructs registry-compatible .proto text from generated descriptors. */
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
        for (var dependency : file.getDependencies()) {
            output.append("import \"").append(escape(dependency.getName())).append("\";\n");
        }
        appendOptions(output, file.getOptions(), "", "option ");
        file.getEnumTypes().forEach(value -> appendEnum(output, value, ""));
        file.getMessageTypes().forEach(value -> appendMessage(output, value, ""));
        file.getExtensions().forEach(value -> appendExtension(output, value, ""));
        file.getServices().forEach(service -> {
            output.append("\nservice ").append(service.getName()).append(" {\n");
            appendOptions(output, service.getOptions(), "  ", "option ");
            service.getMethods().forEach(method -> {
                output.append("  rpc ").append(method.getName()).append(" (");
                if (method.isClientStreaming()) {
                    output.append("stream ");
                }
                output.append('.').append(method.getInputType().getFullName()).append(") returns (");
                if (method.isServerStreaming()) {
                    output.append("stream ");
                }
                output.append('.').append(method.getOutputType().getFullName()).append(")");
                if (method.getOptions().getAllFields().isEmpty()) {
                    output.append(";\n");
                } else {
                    output.append(" {\n");
                    appendOptions(output, method.getOptions(), "    ", "option ");
                    output.append("  }\n");
                }
            });
            output.append("}\n");
        });
        return output.toString();
    }

    private static void appendMessage(StringBuilder output, Descriptor message, String indent) {
        if (message.getOptions().getMapEntry()) {
            return;
        }
        output.append('\n').append(indent).append("message ").append(message.getName()).append(" {\n");
        var inner = indent + "  ";
        appendOptions(output, message.getOptions(), inner, "option ");
        message.getEnumTypes().forEach(value -> appendEnum(output, value, inner));
        message.getNestedTypes().stream()
                .filter(nested -> message.getFields().stream().noneMatch(field ->
                        field.getType() == FieldDescriptor.Type.GROUP && field.getMessageType() == nested))
                .forEach(value -> appendMessage(output, value, inner));
        for (var range : message.toProto().getReservedRangeList()) {
            output.append(inner).append("reserved ").append(range.getStart()).append(" to ")
                    .append(range.getEnd() - 1).append(";\n");
        }
        if (!message.toProto().getReservedNameList().isEmpty()) {
            output.append(inner).append("reserved ")
                    .append(message.toProto().getReservedNameList().stream()
                            .map(name -> "\"" + escape(name) + "\"")
                            .collect(java.util.stream.Collectors.joining(", ")))
                    .append(";\n");
        }
        for (var range : message.toProto().getExtensionRangeList()) {
            output.append(inner).append("extensions ").append(range.getStart()).append(" to ")
                    .append(range.getEnd() - 1).append(";\n");
        }
        message.getFields().stream()
                .filter(field -> field.getContainingOneof() == null || field.toProto().getProto3Optional())
                .forEach(field -> appendField(output, field, inner));
        message.getOneofs().stream().filter(oneof -> !(oneof.getFieldCount() == 1
                && oneof.getFields().get(0).toProto().getProto3Optional())).forEach(oneof -> {
            output.append(inner).append("oneof ").append(oneof.getName()).append(" {\n");
            appendOptions(output, oneof.getOptions(), inner + "  ", "option ");
            oneof.getFields().forEach(field -> appendField(output, field, inner + "  "));
            output.append(inner).append("}\n");
        });
        message.getExtensions().forEach(field -> appendExtension(output, field, inner));
        output.append(indent).append("}\n");
    }

    private static void appendEnum(StringBuilder output, EnumDescriptor value, String indent) {
        output.append('\n').append(indent).append("enum ").append(value.getName()).append(" {\n");
        appendOptions(output, value.getOptions(), indent + "  ", "option ");
        value.getValues().forEach(item -> {
            output.append(indent).append("  ").append(item.getName()).append(" = ").append(item.getNumber());
            appendInlineOptions(output, item.getOptions());
            output.append(";\n");
        });
        output.append(indent).append("}\n");
    }

    private static void appendExtension(StringBuilder output, FieldDescriptor field, String indent) {
        output.append(indent).append("extend .").append(field.getContainingType().getFullName()).append(" {\n");
        appendField(output, field, indent + "  ");
        output.append(indent).append("}\n");
    }

    private static void appendField(StringBuilder output, FieldDescriptor field, String indent) {
        output.append(indent);
        if (field.toProto().getProto3Optional()) {
            output.append("optional ");
        } else if (field.getContainingOneof() != null) {
            // oneof members have no label in proto2 or proto3
        } else if (field.isRepeated() && !field.isMapField()) {
            output.append("repeated ");
        } else if (isProto2(field.getFile()) && field.isRequired()) {
            output.append("required ");
        } else if (isProto2(field.getFile())) {
            output.append("optional ");
        }
        if (field.getType() == FieldDescriptor.Type.GROUP) {
            output.append("group ").append(field.getMessageType().getName()).append(" = ")
                    .append(field.getNumber()).append(" {\n");
            field.getMessageType().getFields().forEach(child -> appendField(output, child, indent + "  "));
            output.append(indent).append("}\n");
            return;
        }
        output.append(typeName(field)).append(' ').append(field.getName()).append(" = ").append(field.getNumber());
        appendInlineOptions(output, field.getOptions());
        output.append(";\n");
    }

    private static String typeName(FieldDescriptor field) {
        if (field.isMapField()) {
            var key = field.getMessageType().findFieldByNumber(1);
            var value = field.getMessageType().findFieldByNumber(2);
            return "map<" + typeName(key) + ", " + typeName(value) + ">";
        }
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            return "." + field.getMessageType().getFullName();
        }
        if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
            return "." + field.getEnumType().getFullName();
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

    private static void appendOptions(StringBuilder output, Message options, String indent, String prefix) {
        options.getAllFields().forEach((field, value) -> values(value).forEach(item -> output
                .append(indent)
                .append(prefix)
                .append(optionName(field))
                .append(" = ")
                .append(optionValue(item))
                .append(";\n")));
    }

    private static void appendInlineOptions(StringBuilder output, Message options) {
        var rendered = new ArrayList<String>();
        options.getAllFields().forEach((field, value) -> values(value).forEach(item ->
                rendered.add(optionName(field) + " = " + optionValue(item))));
        if (!rendered.isEmpty()) {
            output.append(" [").append(String.join(", ", rendered)).append(']');
        }
    }

    private static List<?> values(Object value) {
        return value instanceof List<?> list ? list : List.of(value);
    }

    private static String optionName(FieldDescriptor field) {
        return field.isExtension() ? "(" + field.getFullName() + ")" : field.getName();
    }

    private static String optionValue(Object value) {
        if (value instanceof String string) {
            return "\"" + escape(string) + "\"";
        }
        if (value instanceof ByteString bytes) {
            return "\"" + escape(bytes.toStringUtf8()) + "\"";
        }
        if (value instanceof GenericDescriptor descriptor) {
            return descriptor.getName();
        }
        return value.toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    private static boolean isProto2(FileDescriptor file) {
        return "proto2".equals(file.toProto().getSyntax());
    }
}
