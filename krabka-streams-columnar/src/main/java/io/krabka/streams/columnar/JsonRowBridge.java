package io.krabka.streams.columnar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/** Converts JSON-compatible Java records to flat Arrow columns. */
public final class JsonRowBridge<T> implements RowBridge<T> {
    private static final String JSON_METADATA = "krabka.json";
    private static final String BINARY_METADATA = "krabka.binary";

    private final Class<T> type;
    private final ObjectMapper objectMapper;
    private final boolean scalar;
    private List<Field> fields;

    public JsonRowBridge(Class<T> type) {
        this(type, new ObjectMapper());
    }

    public JsonRowBridge(Class<T> type, ObjectMapper objectMapper) {
        this(type, objectMapper, (List<Field>) null);
    }

    public JsonRowBridge(Class<T> type, Schema schema) {
        this(type, new ObjectMapper(), Objects.requireNonNull(schema, "schema").getFields());
    }

    public JsonRowBridge(Class<T> type, ObjectMapper objectMapper, Schema schema) {
        this(type, objectMapper, Objects.requireNonNull(schema, "schema").getFields());
    }

    /** Creates a bridge whose Arrow fields come from a JSON Schema instead of sample rows. */
    public static <T> JsonRowBridge<T> fromJsonSchema(Class<T> type, String jsonSchema) {
        return fromJsonSchema(type, jsonSchema, new ObjectMapper());
    }

    public static <T> JsonRowBridge<T> fromJsonSchema(
            Class<T> type, String jsonSchema, ObjectMapper objectMapper) {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(objectMapper, "objectMapper");
        try {
            var root = objectMapper.readTree(Objects.requireNonNull(jsonSchema, "jsonSchema"));
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException("JSON Schema must be an object");
            }
            return new JsonRowBridge<>(type, objectMapper, jsonSchemaFields(root, isScalar(type)));
        } catch (java.io.IOException error) {
            throw new IllegalArgumentException("invalid JSON Schema", error);
        }
    }

    private JsonRowBridge(Class<T> type, ObjectMapper objectMapper, List<Field> fields) {
        this.type = Objects.requireNonNull(type, "type");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.fields = fields == null ? null : List.copyOf(fields);
        this.scalar = isScalar(type);
    }

    @Override
    public synchronized VectorSchemaRoot rowsToBatch(List<T> rows, BufferAllocator allocator) {
        var objects = rows.stream().map(this::objectNode).toList();
        var samples = new LinkedHashMap<String, List<JsonNode>>();
        for (var object : objects) {
            object.properties().forEach(entry -> samples
                    .computeIfAbsent(entry.getKey(), ignored -> new ArrayList<>())
                    .add(entry.getValue()));
        }
        var batchFields = fields;
        if (batchFields == null) {
            batchFields = samples.entrySet().stream()
                    .map(entry -> field(entry.getKey(), entry.getValue()))
                    .toList();
            if (!rows.isEmpty()) {
                fields = batchFields;
            }
        }
        var root = ArrowBatchSupport.create(batchFields, rows.size(), allocator);
        try {
            for (int column = 0; column < batchFields.size(); column++) {
                var field = batchFields.get(column);
                var vector = root.getVector(column);
                for (int row = 0; row < objects.size(); row++) {
                    writeJson(vector, row, objects.get(row).get(field.getName()), field);
                }
            }
            ArrowBatchSupport.setValueCounts(root);
            return root;
        } catch (RuntimeException error) {
            root.close();
            throw error;
        }
    }

    @Override
    public List<T> batchToRows(VectorSchemaRoot batch) {
        var rows = new ArrayList<T>(batch.getRowCount());
        for (int row = 0; row < batch.getRowCount(); row++) {
            var object = objectMapper.createObjectNode();
            for (var vector : batch.getFieldVectors()) {
                object.set(vector.getName(), readJson(vector, row));
            }
            try {
                var source = scalar ? object.get("value") : object;
                rows.add(objectMapper.treeToValue(source, type));
            } catch (Exception error) {
                throw new ColumnarException("cannot convert Arrow row " + row + " to " + type.getName(), error);
            }
        }
        return List.copyOf(rows);
    }

    private ObjectNode objectNode(T row) {
        var value = objectMapper.valueToTree(row);
        if (!scalar && value instanceof ObjectNode object) {
            return object;
        }
        return objectMapper.createObjectNode().set("value", value);
    }

    private static Field field(String name, List<JsonNode> values) {
        var sample = values.stream().filter(value -> value != null && !value.isNull()).findFirst().orElse(null);
        ArrowType arrowType;
        Map<String, String> metadata = Map.of();
        if (sample == null || sample.isTextual()) {
            arrowType = new ArrowType.Utf8();
        } else if (sample.isIntegralNumber()) {
            arrowType = new ArrowType.Int(64, true);
        } else if (sample.isFloatingPointNumber()) {
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (sample.isBoolean()) {
            arrowType = new ArrowType.Bool();
        } else if (sample.isBinary()) {
            arrowType = new ArrowType.Binary();
            metadata = Map.of(BINARY_METADATA, "true");
        } else {
            arrowType = new ArrowType.Utf8();
            metadata = Map.of(JSON_METADATA, "true");
        }
        return new Field(name, new FieldType(true, arrowType, null, metadata), null);
    }

    private void writeJson(FieldVector vector, int row, JsonNode value, Field field) {
        if (value == null || value.isNull()) {
            if (!field.isNullable()) {
                throw new ColumnarException("required JSON field is null: " + field.getName());
            }
            ArrowBatchSupport.setValue(vector, row, null);
            return;
        }
        try {
            Object converted;
            if ("true".equals(field.getMetadata().get(JSON_METADATA))) {
                converted = objectMapper.writeValueAsString(value);
            } else if ("true".equals(field.getMetadata().get(BINARY_METADATA))) {
                converted = value.binaryValue();
            } else if (value.isIntegralNumber()) {
                converted = value.longValue();
            } else if (value.isFloatingPointNumber()) {
                converted = value.doubleValue();
            } else if (value.isBoolean()) {
                converted = value.booleanValue();
            } else {
                converted = value.asText();
            }
            ArrowBatchSupport.setValue(vector, row, converted);
        } catch (Exception error) {
            throw new ColumnarException("cannot write JSON field " + field.getName(), error);
        }
    }

    private JsonNode readJson(FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return objectMapper.nullNode();
        }
        var field = vector.getField();
        var value = ArrowBatchSupport.value(vector, row);
        try {
            if ("true".equals(field.getMetadata().get(JSON_METADATA))) {
                return objectMapper.readTree(value.toString());
            }
            if ("true".equals(field.getMetadata().get(BINARY_METADATA))) {
                var buffer = (java.nio.ByteBuffer) value;
                var bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                return objectMapper.getNodeFactory().binaryNode(bytes);
            }
            if (value instanceof Boolean bool) {
                return objectMapper.getNodeFactory().booleanNode(bool);
            }
            if (value instanceof Float || value instanceof Double) {
                return objectMapper.getNodeFactory().numberNode(((Number) value).doubleValue());
            }
            if (value instanceof Number number) {
                return objectMapper.getNodeFactory().numberNode(number.longValue());
            }
            return objectMapper.getNodeFactory().textNode(value.toString());
        } catch (Exception error) {
            throw new ColumnarException("cannot read JSON field " + field.getName(), error);
        }
    }

    private static boolean isScalar(Class<?> type) {
        return type.isPrimitive()
                || type.isArray()
                || CharSequence.class.isAssignableFrom(type)
                || Number.class.isAssignableFrom(type)
                || Boolean.class == type;
    }

    private static List<Field> jsonSchemaFields(JsonNode root, boolean scalar) {
        if (scalar) {
            return List.of(jsonSchemaField("value", root, false, root));
        }
        var properties = root.path("properties");
        if (!properties.isObject()) {
            throw new IllegalArgumentException("object JSON Schema has no properties");
        }
        var required = new java.util.HashSet<String>();
        root.path("required").forEach(value -> required.add(value.asText()));
        var fields = new ArrayList<Field>();
        properties.properties().forEach(entry -> fields.add(jsonSchemaField(
                entry.getKey(), entry.getValue(), !required.contains(entry.getKey()), root)));
        return List.copyOf(fields);
    }

    private static Field jsonSchemaField(
            String name, JsonNode declaration, boolean nullable, JsonNode root) {
        var resolved = resolve(declaration, root);
        var typeNode = resolved.path("type");
        String type = null;
        if (typeNode.isTextual()) {
            type = typeNode.asText();
        } else if (typeNode.isArray()) {
            for (var candidate : typeNode) {
                if (candidate.isTextual() && !"null".equals(candidate.asText())) {
                    type = candidate.asText();
                    break;
                }
            }
            nullable = nullable || java.util.stream.StreamSupport.stream(typeNode.spliterator(), false)
                    .anyMatch(candidate -> "null".equals(candidate.asText()));
        }
        ArrowType arrowType;
        Map<String, String> metadata = Map.of();
        if ("integer".equals(type)) {
            arrowType = new ArrowType.Int(64, true);
        } else if ("number".equals(type)) {
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if ("boolean".equals(type)) {
            arrowType = new ArrowType.Bool();
        } else if ("object".equals(type) || "array".equals(type)) {
            arrowType = new ArrowType.Utf8();
            metadata = Map.of(JSON_METADATA, "true");
        } else if ("base64".equals(resolved.path("contentEncoding").asText())) {
            arrowType = new ArrowType.Binary();
            metadata = Map.of(BINARY_METADATA, "true");
        } else {
            arrowType = new ArrowType.Utf8();
        }
        return new Field(name, new FieldType(nullable, arrowType, null, metadata), null);
    }

    private static JsonNode resolve(JsonNode declaration, JsonNode root) {
        var reference = declaration.path("$ref");
        if (!reference.isTextual() || !reference.asText().startsWith("#/")) {
            return declaration;
        }
        var resolved = root.at(reference.asText().substring(1));
        if (resolved.isMissingNode()) {
            throw new IllegalArgumentException("unresolved JSON Schema reference " + reference.asText());
        }
        return resolved;
    }
}
