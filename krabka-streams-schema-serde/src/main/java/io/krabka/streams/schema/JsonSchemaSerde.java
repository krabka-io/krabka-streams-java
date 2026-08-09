package io.krabka.streams.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.errors.SerializationException;

/** A Kafka serde for Confluent-framed JSON Schema values. */
public final class JsonSchemaSerde<T> extends AbstractSchemaSerde<T> {
    private final Class<T> type;
    private final ObjectMapper objectMapper;
    private final boolean validate;
    private final SchemaRegistry validatorRegistry;
    private final Schema localValidator;
    private final ConcurrentMap<Integer, Schema> validators = new ConcurrentHashMap<>();

    private JsonSchemaSerde(
            Class<T> type,
            String schema,
            SchemaCache cache,
            Role role,
            boolean validate,
            ObjectMapper objectMapper,
            SpecificationVersion dialect,
            SubjectNameStrategy strategy) {
        super(cache, role, SchemaKind.JSON, schema, null, strategy);
        this.type = Objects.requireNonNull(type, "type");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.validate = validate;
        var detectedDialect = dialect;
        if (detectedDialect == null) {
            try {
                detectedDialect = SpecificationVersion.fromSchemaNode(objectMapper.readTree(schema))
                        .orElse(SpecificationVersion.DRAFT_2020_12);
            } catch (java.io.IOException error) {
                throw new IllegalArgumentException("invalid JSON Schema", error);
            }
        }
        this.validatorRegistry = SchemaRegistry.withDefaultDialect(detectedDialect);
        this.localValidator = validate ? validatorRegistry.getSchema(schema, InputFormat.JSON) : null;
    }

    public static <T> JsonSchemaSerde<T> forValue(
            Class<T> type, String schema, SchemaCache cache, boolean validate) {
        return new JsonSchemaSerde<>(
                type, schema, cache, Role.VALUE, validate, new ObjectMapper(), null, null);
    }

    public static <T> JsonSchemaSerde<T> forKey(
            Class<T> type, String schema, SchemaCache cache, boolean validate) {
        return new JsonSchemaSerde<>(
                type, schema, cache, Role.KEY, validate, new ObjectMapper(), null, null);
    }

    public static <T> JsonSchemaSerde<T> forValue(
            Class<T> type, String schema, SchemaCache cache, boolean validate, ObjectMapper objectMapper) {
        return new JsonSchemaSerde<>(
                type, schema, cache, Role.VALUE, validate, objectMapper, null, null);
    }

    public static <T> JsonSchemaSerde<T> forValue(
            Class<T> type,
            String schema,
            SchemaCache cache,
            boolean validate,
            SpecificationVersion dialect,
            SubjectNameStrategy strategy,
            ObjectMapper objectMapper) {
        return new JsonSchemaSerde<>(
                type, schema, cache, Role.VALUE, validate, objectMapper, dialect, strategy);
    }

    public static <T> JsonSchemaSerde<T> forKey(
            Class<T> type,
            String schema,
            SchemaCache cache,
            boolean validate,
            SpecificationVersion dialect,
            SubjectNameStrategy strategy,
            ObjectMapper objectMapper) {
        return new JsonSchemaSerde<>(type, schema, cache, Role.KEY, validate, objectMapper, dialect, strategy);
    }

    @Override
    protected byte[] serializeBody(T value) throws Exception {
        var body = objectMapper.writeValueAsBytes(value);
        if (validate) {
            validate(localValidator, body);
        }
        return body;
    }

    @Override
    protected T deserializeBody(int schemaId, byte[] body) throws Exception {
        var writerSchema = cache().writerSchema(schemaId);
        if (validate) {
            var validator = validators.computeIfAbsent(
                    schemaId, ignored -> validatorRegistry.getSchema(writerSchema, InputFormat.JSON));
            validate(validator, body);
        }
        return objectMapper.readValue(body, type);
    }

    private static void validate(Schema validator, byte[] body) {
        var errors = validator.validate(
                new String(body, java.nio.charset.StandardCharsets.UTF_8), InputFormat.JSON);
        if (!errors.isEmpty()) {
            throw new SerializationException("JSON Schema validation failed: " + errors.get(0).getMessage());
        }
    }
}
