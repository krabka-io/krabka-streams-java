package io.krabka.streams.schema;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;

/** A Kafka serde for Confluent-framed Protobuf messages. */
public final class ProtobufSerde<T extends Message> extends AbstractSchemaSerde<T> {
    private final T defaultInstance;
    private final Parser<T> parser;
    private final List<Integer> messageIndexes;

    private ProtobufSerde(T defaultInstance, SchemaCache cache, Role role) {
        super(
                cache,
                role,
                SchemaKind.PROTOBUF,
                ProtobufSchemaPrinter.print(defaultInstance.getDescriptorForType().getFile()),
                defaultInstance.getDescriptorForType().getFullName());
        this.defaultInstance = Objects.requireNonNull(defaultInstance, "defaultInstance");
        this.parser = parser(defaultInstance);
        this.messageIndexes = List.of(defaultInstance.getDescriptorForType().getIndex());
    }

    public static <T extends Message> ProtobufSerde<T> forValue(T defaultInstance, SchemaCache cache) {
        return new ProtobufSerde<>(defaultInstance, cache, Role.VALUE);
    }

    public static <T extends Message> ProtobufSerde<T> forKey(T defaultInstance, SchemaCache cache) {
        return new ProtobufSerde<>(defaultInstance, cache, Role.KEY);
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
}
