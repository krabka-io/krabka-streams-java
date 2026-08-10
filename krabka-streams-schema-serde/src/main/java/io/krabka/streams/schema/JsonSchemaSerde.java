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

/**
 * A Kafka serde for Confluent-framed JSON Schema values.
 *
 * <p>Values are bound to and from JSON with Jackson, so any Jackson-compatible class
 * works, including Java records. Optional validation checks the serialized document
 * against the local schema when writing and against the registered writer schema when
 * reading; a validation failure throws {@link SerializationException}.
 *
 * <p>The schema dialect is detected from the document's {@code $schema} keyword and
 * falls back to draft 2020-12; pass an explicit
 * {@link SpecificationVersion} through the long factory overloads to pin it.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * record Order(String user, long amount) {}
 *
 * String orderSchema = """
 *     {"type": "object",
 *      "properties": {"user": {"type": "string"}, "amount": {"type": "integer"}},
 *      "required": ["user", "amount"]}""";
 *
 * var serde = JsonSchemaSerde.forValue(Order.class, orderSchema, cache, true);
 * serde.registerSubject("orders");
 * cache.prewarm().join();
 *
 * byte[] bytes = serde.serializer().serialize("orders", new Order("ada", 7));
 * }</pre>
 *
 * @param <T> the Jackson-compatible type carried by the serde
 */
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

    /**
     * Creates a value serde with a default Jackson mapper.
     *
     * @param <T> the Jackson-compatible value type
     * @param type the class values are bound to and from
     * @param schema the JSON Schema document registered for the subject
     * @param cache the cache that resolves subjects and writer schemas
     * @param validate whether to validate documents on both serialize and deserialize
     * @return a value serde for {@code type}
     * @throws IllegalArgumentException if {@code schema} is not valid JSON
     */
    public static <T> JsonSchemaSerde<T> forValue(
            Class<T> type, String schema, SchemaCache cache, boolean validate) {
        return new JsonSchemaSerde<>(
                type, schema, cache, Role.VALUE, validate, new ObjectMapper(), null, null);
    }

    /**
     * Creates a key serde with a default Jackson mapper.
     *
     * @param <T> the Jackson-compatible key type
     * @param type the class keys are bound to and from
     * @param schema the JSON Schema document registered for the subject
     * @param cache the cache that resolves subjects and writer schemas
     * @param validate whether to validate documents on both serialize and deserialize
     * @return a key serde for {@code type}
     * @throws IllegalArgumentException if {@code schema} is not valid JSON
     */
    public static <T> JsonSchemaSerde<T> forKey(
            Class<T> type, String schema, SchemaCache cache, boolean validate) {
        return new JsonSchemaSerde<>(
                type, schema, cache, Role.KEY, validate, new ObjectMapper(), null, null);
    }

    /**
     * Creates a value serde with a caller-supplied Jackson mapper.
     *
     * @param <T> the Jackson-compatible value type
     * @param type the class values are bound to and from
     * @param schema the JSON Schema document registered for the subject
     * @param cache the cache that resolves subjects and writer schemas
     * @param validate whether to validate documents on both serialize and deserialize
     * @param objectMapper the mapper used for binding and validation input
     * @return a value serde for {@code type}
     * @throws IllegalArgumentException if {@code schema} is not valid JSON
     */
    public static <T> JsonSchemaSerde<T> forValue(
            Class<T> type, String schema, SchemaCache cache, boolean validate, ObjectMapper objectMapper) {
        return new JsonSchemaSerde<>(
                type, schema, cache, Role.VALUE, validate, objectMapper, null, null);
    }

    /**
     * Creates a fully configured value serde.
     *
     * @param <T> the Jackson-compatible value type
     * @param type the class values are bound to and from
     * @param schema the JSON Schema document registered for the subject
     * @param cache the cache that resolves subjects and writer schemas
     * @param validate whether to validate documents on both serialize and deserialize
     * @param dialect the JSON Schema dialect, or null to detect it from the document
     * @param strategy the subject naming strategy, or null for the cache default
     * @param objectMapper the mapper used for binding and validation input
     * @return a value serde for {@code type}
     * @throws IllegalArgumentException if {@code schema} is not valid JSON
     */
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

    /**
     * Creates a fully configured key serde.
     *
     * @param <T> the Jackson-compatible key type
     * @param type the class keys are bound to and from
     * @param schema the JSON Schema document registered for the subject
     * @param cache the cache that resolves subjects and writer schemas
     * @param validate whether to validate documents on both serialize and deserialize
     * @param dialect the JSON Schema dialect, or null to detect it from the document
     * @param strategy the subject naming strategy, or null for the cache default
     * @param objectMapper the mapper used for binding and validation input
     * @return a key serde for {@code type}
     * @throws IllegalArgumentException if {@code schema} is not valid JSON
     */
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
