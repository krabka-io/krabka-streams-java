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
    private static final SchemaRegistry VALIDATOR =
            SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);

    private final Class<T> type;
    private final ObjectMapper objectMapper;
    private final boolean validate;
    private final ConcurrentMap<Integer, Schema> validators = new ConcurrentHashMap<>();

    private JsonSchemaSerde(
            Class<T> type,
            String schema,
            SchemaCache cache,
            Role role,
            boolean validate,
            ObjectMapper objectMapper) {
        super(cache, role, SchemaKind.JSON, schema, null);
        this.type = Objects.requireNonNull(type, "type");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.validate = validate;
    }

    public static <T> JsonSchemaSerde<T> forValue(
            Class<T> type, String schema, SchemaCache cache, boolean validate) {
        return new JsonSchemaSerde<>(type, schema, cache, Role.VALUE, validate, new ObjectMapper());
    }

    public static <T> JsonSchemaSerde<T> forKey(
            Class<T> type, String schema, SchemaCache cache, boolean validate) {
        return new JsonSchemaSerde<>(type, schema, cache, Role.KEY, validate, new ObjectMapper());
    }

    public static <T> JsonSchemaSerde<T> forValue(
            Class<T> type, String schema, SchemaCache cache, boolean validate, ObjectMapper objectMapper) {
        return new JsonSchemaSerde<>(type, schema, cache, Role.VALUE, validate, objectMapper);
    }

    @Override
    protected byte[] serializeBody(T value) throws Exception {
        return objectMapper.writeValueAsBytes(value);
    }

    @Override
    protected T deserializeBody(int schemaId, byte[] body) throws Exception {
        var writerSchema = cache().writerSchema(schemaId);
        if (validate) {
            var validator = validators.computeIfAbsent(
                    schemaId, ignored -> VALIDATOR.getSchema(writerSchema, InputFormat.JSON));
            var errors = validator.validate(new String(body, java.nio.charset.StandardCharsets.UTF_8), InputFormat.JSON);
            if (!errors.isEmpty()) {
                throw new SerializationException("JSON Schema validation failed: " + errors.get(0).getMessage());
            }
        }
        return objectMapper.readValue(body, type);
    }
}
