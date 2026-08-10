package io.krabka.streams.schema;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;

/**
 * A Kafka serde for Confluent-framed Protobuf messages.
 *
 * <p>The serde is created from a generated message's default instance. The
 * {@code .proto} schema text registered with the registry is reconstructed from the
 * message's file descriptor, and the Confluent Protobuf frame — including the
 * message-index list that identifies nested messages — is written and read
 * automatically.
 *
 * <p>On deserialization the writer's registered {@code messageType} is compared with
 * the local message's full name; a mismatch throws {@link SerializationException}
 * instead of silently parsing bytes into the wrong message.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var serde = ProtobufSerde.forValue(Order.getDefaultInstance(), cache);
 * serde.registerSubject("orders");
 * cache.prewarm().join();
 *
 * byte[] bytes = serde.serializer().serialize("orders", order);
 * Order roundTripped = serde.deserializer().deserialize("orders", bytes);
 * }</pre>
 *
 * @param <T> the generated Protobuf message type
 */
public final class ProtobufSerde<T extends Message> extends AbstractSchemaSerde<T> {
    private final T defaultInstance;
    private final Parser<T> parser;
    private final List<Integer> messageIndexes;

    private ProtobufSerde(T defaultInstance, SchemaCache cache, Role role) {
        this(defaultInstance, cache, role, null);
    }

    private ProtobufSerde(
            T defaultInstance, SchemaCache cache, Role role, SubjectNameStrategy strategy) {
        super(
                cache,
                role,
                SchemaKind.PROTOBUF,
                ProtobufSchemaPrinter.print(defaultInstance.getDescriptorForType().getFile()),
                defaultInstance.getDescriptorForType().getFullName(),
                strategy);
        this.defaultInstance = Objects.requireNonNull(defaultInstance, "defaultInstance");
        this.parser = parser(defaultInstance);
        this.messageIndexes = messageIndexes(defaultInstance.getDescriptorForType());
    }

    /**
     * Creates a value serde for a generated Protobuf message.
     *
     * @param <T> the generated Protobuf message type
     * @param defaultInstance the message's default instance, for example {@code Order.getDefaultInstance()}
     * @param cache the cache that resolves subjects and writer schemas
     * @return a value serde for the message type
     */
    public static <T extends Message> ProtobufSerde<T> forValue(T defaultInstance, SchemaCache cache) {
        return new ProtobufSerde<>(defaultInstance, cache, Role.VALUE);
    }

    /**
     * Creates a value serde with a custom subject strategy.
     *
     * @param <T> the generated Protobuf message type
     * @param defaultInstance the message's default instance, for example {@code Order.getDefaultInstance()}
     * @param cache the cache that resolves subjects and writer schemas
     * @param strategy the subject naming strategy that overrides the cache default
     * @return a value serde for the message type
     */
    public static <T extends Message> ProtobufSerde<T> forValue(
            T defaultInstance, SchemaCache cache, SubjectNameStrategy strategy) {
        return new ProtobufSerde<>(defaultInstance, cache, Role.VALUE, strategy);
    }

    /**
     * Creates a key serde for a generated Protobuf message.
     *
     * @param <T> the generated Protobuf message type
     * @param defaultInstance the message's default instance, for example {@code OrderKey.getDefaultInstance()}
     * @param cache the cache that resolves subjects and writer schemas
     * @return a key serde for the message type
     */
    public static <T extends Message> ProtobufSerde<T> forKey(T defaultInstance, SchemaCache cache) {
        return new ProtobufSerde<>(defaultInstance, cache, Role.KEY);
    }

    /**
     * Creates a key serde with a custom subject strategy.
     *
     * @param <T> the generated Protobuf message type
     * @param defaultInstance the message's default instance, for example {@code OrderKey.getDefaultInstance()}
     * @param cache the cache that resolves subjects and writer schemas
     * @param strategy the subject naming strategy that overrides the cache default
     * @return a key serde for the message type
     */
    public static <T extends Message> ProtobufSerde<T> forKey(
            T defaultInstance, SchemaCache cache, SubjectNameStrategy strategy) {
        return new ProtobufSerde<>(defaultInstance, cache, Role.KEY, strategy);
    }

    @Override
    protected byte[] serializeBody(T value) {
        return value.toByteArray();
    }

    @Override
    protected T deserializeBody(int schemaId, byte[] body) throws Exception {
        cache().writerSchema(schemaId);
        var writerMessageType = cache().writerMessageType(schemaId);
        var localMessageType = defaultInstance.getDescriptorForType().getFullName();
        if (writerMessageType != null && !writerMessageType.equals(localMessageType)) {
            throw new SerializationException(
                    "Protobuf messageType mismatch: writer " + writerMessageType + ", local " + localMessageType);
        }
        return parser.parseFrom(body);
    }

    @Override
    protected byte[] frame(int schemaId, byte[] body) {
        return ConfluentWireFormat.encodeProtobuf(schemaId, messageIndexes, body);
    }

    @Override
    protected ConfluentWireFormat.Frame unframe(byte[] bytes) {
        var frame = ConfluentWireFormat.decodeProtobuf(bytes);
        return new ConfluentWireFormat.Frame(frame.schemaId(), frame.body());
    }

    @SuppressWarnings("unchecked")
    private static <T extends Message> Parser<T> parser(T defaultInstance) {
        return (Parser<T>) defaultInstance.getParserForType();
    }

    private static List<Integer> messageIndexes(Descriptor descriptor) {
        var indexes = new ArrayList<Integer>();
        for (var current = descriptor; current != null; current = current.getContainingType()) {
            indexes.add(current.getIndex());
        }
        Collections.reverse(indexes);
        return List.copyOf(indexes);
    }
}
