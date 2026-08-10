package io.krabka.streams.schema;

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * The shared skeleton of the Confluent-framed serdes.
 *
 * <p>Subclasses supply the schema-format-specific body encoding; this class supplies
 * the Confluent wire framing, the subject resolution through a {@link SchemaCache},
 * and the key-versus-value role check that runs when Kafka configures the serde.
 *
 * <p>Serialization is synchronous and never talks to the registry: the schema ID must
 * already be resolved, which is why applications call {@link #registerSubject(String)}
 * for every topic and then {@link SchemaCache#prewarm()} before producing.
 * Deserialization of an unknown schema ID starts one background fetch and throws the
 * retriable {@link SchemaFetchPendingException} until the fetch completes.
 *
 * @param <T> the Java type carried by the serde
 */
abstract class AbstractSchemaSerde<T> implements Serde<T> {
    private final SchemaCache cache;
    private final Role role;
    private final SchemaKind kind;
    private final String schema;
    private final String messageType;
    private final SubjectNameStrategy subjectNameStrategy;
    private final Serializer<T> serializer = new SchemaSerializer();
    private final Deserializer<T> deserializer = new SchemaDeserializer();

    AbstractSchemaSerde(SchemaCache cache, Role role, SchemaKind kind, String schema, String messageType) {
        this(cache, role, kind, schema, messageType, null);
    }

    AbstractSchemaSerde(
            SchemaCache cache,
            Role role,
            SchemaKind kind,
            String schema,
            String messageType,
            SubjectNameStrategy subjectNameStrategy) {
        this.cache = Objects.requireNonNull(cache, "cache");
        this.role = Objects.requireNonNull(role, "role");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.messageType = messageType;
        this.subjectNameStrategy = subjectNameStrategy;
    }

    /**
     * Adds this serde's subject to the cache prewarm set.
     *
     * <p>Call this once per topic the serde will read or write, then resolve every
     * registered subject with {@link SchemaCache#prewarm()} before the first record is
     * serialized. Registration is idempotent by subject.
     *
     * @param topic the Kafka topic whose subject should be resolved during prewarm
     */
    public final void registerSubject(String topic) {
        cache.intern(subject(topic), kind, schema, messageType);
    }

    /**
     * Returns the schema cache this serde resolves subjects and writer schemas with.
     *
     * @return the schema cache supplied at construction
     */
    protected final SchemaCache cache() {
        return cache;
    }

    /**
     * Returns the serializer half of this serde.
     *
     * <p>The serializer frames each non-null value with the Confluent wire format
     * using the schema ID resolved during prewarm. It throws
     * {@link SerializationException} when the subject has not been resolved yet or
     * when the body cannot be encoded. A null value serializes to null.
     *
     * @return the reusable, thread-safe serializer
     */
    @Override
    public final Serializer<T> serializer() {
        return serializer;
    }

    /**
     * Returns the deserializer half of this serde.
     *
     * <p>The deserializer unframes the Confluent header, resolves the writer schema
     * for the frame's schema ID through the cache, and decodes the body. It throws
     * {@link SchemaFetchPendingException} while an unknown writer schema is being
     * fetched and {@link SerializationException} for malformed input. Null input
     * deserializes to null.
     *
     * @return the reusable, thread-safe deserializer
     */
    @Override
    public final Deserializer<T> deserializer() {
        return deserializer;
    }

    /**
     * Encodes one value into the schema-format-specific body bytes, without framing.
     *
     * @param value the non-null value to encode
     * @return the encoded body
     * @throws Exception if the value cannot be encoded
     */
    protected abstract byte[] serializeBody(T value) throws Exception;

    /**
     * Decodes one body into a value using the writer schema registered under the ID.
     *
     * @param schemaId the schema ID taken from the frame header
     * @param body the body bytes that followed the frame header
     * @return the decoded value
     * @throws Exception if the body cannot be decoded
     */
    protected abstract T deserializeBody(int schemaId, byte[] body) throws Exception;

    /**
     * Wraps body bytes in the Confluent wire format.
     *
     * @param schemaId the resolved schema ID for the subject
     * @param body the encoded body
     * @return the framed record bytes
     */
    protected byte[] frame(int schemaId, byte[] body) {
        return ConfluentWireFormat.encode(schemaId, body);
    }

    /**
     * Splits framed record bytes into the schema ID and body.
     *
     * @param bytes the framed record bytes
     * @return the decoded frame
     */
    protected ConfluentWireFormat.Frame unframe(byte[] bytes) {
        return ConfluentWireFormat.decode(bytes);
    }

    private int schemaId(String topic) {
        var subject = subject(topic);
        return cache.idForSubject(subject)
                .orElseThrow(() -> new SerializationException(
                        "schema ID for " + subject + " is not resolved; call registerSubject and prewarm first"));
    }

    private String subject(String topic) {
        return subjectNameStrategy == null
                ? cache.subject(topic, role)
                : cache.subject(topic, role, subjectNameStrategy);
    }

    private final class SchemaSerializer implements Serializer<T> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            if (isKey != (role == Role.KEY)) {
                throw new IllegalArgumentException("serde role does not match the Kafka key setting");
            }
        }

        @Override
        public byte[] serialize(String topic, T value) {
            if (value == null) {
                return null;
            }
            try {
                return frame(schemaId(topic), serializeBody(value));
            } catch (SerializationException error) {
                throw error;
            } catch (Exception error) {
                throw new SerializationException("cannot serialize schema value", error);
            }
        }
    }

    private final class SchemaDeserializer implements Deserializer<T> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            if (isKey != (role == Role.KEY)) {
                throw new IllegalArgumentException("serde role does not match the Kafka key setting");
            }
        }

        @Override
        public T deserialize(String topic, byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            try {
                var frame = unframe(bytes);
                return deserializeBody(frame.schemaId(), frame.body());
            } catch (SerializationException error) {
                throw error;
            } catch (Exception error) {
                throw new SerializationException("cannot deserialize schema value", error);
            }
        }
    }
}
