package io.krabka.streams.schema;

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

abstract class AbstractSchemaSerde<T> implements Serde<T> {
    private final SchemaCache cache;
    private final Role role;
    private final SchemaKind kind;
    private final String schema;
    private final String messageType;
    private final Serializer<T> serializer = new SchemaSerializer();
    private final Deserializer<T> deserializer = new SchemaDeserializer();

    AbstractSchemaSerde(SchemaCache cache, Role role, SchemaKind kind, String schema, String messageType) {
        this.cache = Objects.requireNonNull(cache, "cache");
        this.role = Objects.requireNonNull(role, "role");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.messageType = messageType;
    }

    /** Adds this serde's subject to the cache prewarm set. */
    public final void registerSubject(String topic) {
        cache.intern(cache.subject(topic, role), kind, schema, messageType);
    }

    protected final SchemaCache cache() {
        return cache;
    }

    @Override
    public final Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public final Deserializer<T> deserializer() {
        return deserializer;
    }

    protected abstract byte[] serializeBody(T value) throws Exception;

    protected abstract T deserializeBody(int schemaId, byte[] body) throws Exception;

    protected byte[] frame(int schemaId, byte[] body) {
        return ConfluentWireFormat.encode(schemaId, body);
    }

    protected ConfluentWireFormat.Frame unframe(byte[] bytes) {
        return ConfluentWireFormat.decode(bytes);
    }

    private int schemaId(String topic) {
        var subject = cache.subject(topic, role);
        return cache.idForSubject(subject)
                .orElseThrow(() -> new SerializationException(
                        "schema ID for " + subject + " is not resolved; call registerSubject and prewarm first"));
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
