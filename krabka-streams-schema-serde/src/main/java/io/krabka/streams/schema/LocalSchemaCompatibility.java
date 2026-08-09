package io.krabka.streams.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;

/** Pairwise, network-free schema compatibility checks. */
public final class LocalSchemaCompatibility {
    private static final ObjectMapper JSON = new ObjectMapper();

    private LocalSchemaCompatibility() {
    }

    public enum Mode {
        BACKWARD,
        FORWARD,
        FULL
    }

    public static Result avro(String previousSchema, String candidateSchema, Mode mode) {
        var previous = new Schema.Parser().parse(previousSchema);
        var candidate = new Schema.Parser().parse(candidateSchema);
        var errors = new ArrayList<String>();
        directions(mode).forEach(direction -> {
            var reader = direction == Mode.BACKWARD ? candidate : previous;
            var writer = direction == Mode.BACKWARD ? previous : candidate;
            SchemaCompatibility.checkReaderWriterCompatibility(reader, writer)
                    .getResult()
                    .getIncompatibilities()
                    .forEach(error -> errors.add(direction + ": " + error.getMessage()));
        });
        return new Result(errors.isEmpty(), errors);
    }

    public static Result json(String previousSchema, String candidateSchema, Mode mode) {
        try {
            var previous = JSON.readTree(previousSchema);
            var candidate = JSON.readTree(candidateSchema);
            var errors = new ArrayList<String>();
            directions(mode).forEach(direction -> {
                var reader = direction == Mode.BACKWARD ? candidate : previous;
                var writer = direction == Mode.BACKWARD ? previous : candidate;
                checkJsonReader(reader, writer, "", direction, errors);
            });
            return new Result(errors.isEmpty(), errors);
        } catch (java.io.IOException error) {
            throw new IllegalArgumentException("invalid JSON Schema", error);
        }
    }

    public static Result protobuf(
            Descriptors.FileDescriptor previous,
            Descriptors.FileDescriptor candidate,
            Mode mode) {
        var errors = new ArrayList<String>();
        directions(mode).forEach(direction -> {
            var reader = direction == Mode.BACKWARD ? candidate : previous;
            var writer = direction == Mode.BACKWARD ? previous : candidate;
            checkProtobufReader(reader, writer, direction, errors);
        });
        return new Result(errors.isEmpty(), errors);
    }

    private static List<Mode> directions(Mode mode) {
        Objects.requireNonNull(mode, "mode");
        return mode == Mode.FULL ? List.of(Mode.BACKWARD, Mode.FORWARD) : List.of(mode);
    }

    private static void checkJsonReader(
            JsonNode reader,
            JsonNode writer,
            String path,
            Mode direction,
            List<String> errors) {
        var readerTypes = types(reader);
        var writerTypes = types(writer);
        if (!readerTypes.isEmpty()
                && (writerTypes.isEmpty() || !readerTypes.containsAll(writerTypes))) {
            errors.add(direction + ": " + path + " narrows type from " + writerTypes + " to " + readerTypes);
            return;
        }
        if (!readerTypes.contains("object") && !reader.has("properties")) {
            return;
        }
        var readerRequired = strings(reader.path("required"));
        var writerRequired = strings(writer.path("required"));
        for (var required : readerRequired) {
            if (!writerRequired.contains(required)) {
                errors.add(direction + ": " + child(path, required) + " became required");
            }
        }
        var readerProperties = properties(reader);
        var writerProperties = properties(writer);
        writerProperties.forEach((name, writerProperty) -> {
            var readerProperty = readerProperties.get(name);
            if (readerProperty != null) {
                checkJsonReader(readerProperty, writerProperty, child(path, name), direction, errors);
            } else if (reader.path("additionalProperties").isBoolean()
                    && !reader.path("additionalProperties").booleanValue()) {
                errors.add(direction + ": " + child(path, name) + " is no longer allowed");
            }
        });
    }

    private static void checkProtobufReader(
            Descriptors.FileDescriptor reader,
            Descriptors.FileDescriptor writer,
            Mode direction,
            List<String> errors) {
        var readerMessages = messages(reader);
        var writerMessages = messages(writer);
        writerMessages.forEach((name, writerMessage) -> {
            var readerMessage = readerMessages.get(name);
            if (readerMessage == null) {
                errors.add(direction + ": message " + name + " is absent from the reader");
                return;
            }
            var readerFields = new LinkedHashMap<Integer, Descriptors.FieldDescriptor>();
            readerMessage.getFields().forEach(field -> readerFields.put(field.getNumber(), field));
            for (var writerField : writerMessage.getFields()) {
                var readerField = readerFields.get(writerField.getNumber());
                if (readerField != null
                        && (wireType(readerField) != wireType(writerField)
                                || readerField.isRepeated() != writerField.isRepeated())) {
                    errors.add(direction + ": " + name + " field " + writerField.getNumber()
                            + " changed wire type or cardinality");
                }
            }
            for (var readerField : readerMessage.getFields()) {
                if (readerField.isRequired()
                        && writerMessage.findFieldByNumber(readerField.getNumber()) == null) {
                    errors.add(direction + ": " + name + "." + readerField.getName()
                            + " is required but absent from the writer");
                }
            }
        });
    }

    private static Map<String, Descriptors.Descriptor> messages(Descriptors.FileDescriptor file) {
        var result = new LinkedHashMap<String, Descriptors.Descriptor>();
        file.getMessageTypes().forEach(message -> addMessage(message, result));
        return result;
    }

    private static void addMessage(
            Descriptors.Descriptor message, Map<String, Descriptors.Descriptor> result) {
        result.put(message.getFullName(), message);
        message.getNestedTypes().forEach(nested -> addMessage(nested, result));
    }

    private static int wireType(Descriptors.FieldDescriptor field) {
        return switch (field.getType()) {
            case INT32, INT64, UINT32, UINT64, SINT32, SINT64, BOOL, ENUM -> 0;
            case FIXED64, SFIXED64, DOUBLE -> 1;
            case STRING, BYTES, MESSAGE -> 2;
            case GROUP -> 3;
            case FIXED32, SFIXED32, FLOAT -> 5;
        };
    }

    private static Set<String> types(JsonNode schema) {
        var type = schema.path("type");
        var result = new LinkedHashSet<String>();
        if (type.isTextual()) {
            result.add(type.asText());
        } else if (type.isArray()) {
            type.forEach(value -> result.add(value.asText()));
        }
        return result;
    }

    private static Set<String> strings(JsonNode values) {
        var result = new LinkedHashSet<String>();
        if (values.isArray()) {
            values.forEach(value -> result.add(value.asText()));
        }
        return result;
    }

    private static Map<String, JsonNode> properties(JsonNode schema) {
        var result = new LinkedHashMap<String, JsonNode>();
        schema.path("properties").properties().forEach(entry -> result.put(entry.getKey(), entry.getValue()));
        return result;
    }

    private static String child(String path, String name) {
        return path.isEmpty() ? name : path + "." + name;
    }

    public record Result(boolean compatible, List<String> incompatibilities) {
        public Result {
            incompatibilities = List.copyOf(incompatibilities);
        }
    }
}
