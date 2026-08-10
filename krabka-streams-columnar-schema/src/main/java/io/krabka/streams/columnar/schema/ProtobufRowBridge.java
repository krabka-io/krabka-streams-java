package io.krabka.streams.columnar.schema;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.krabka.streams.columnar.ArrowValues;
import io.krabka.streams.columnar.ColumnarException;
import io.krabka.streams.columnar.RowBridge;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Converts Protobuf messages to and from Arrow batches with descriptor-derived
 * columns.
 *
 * <p>The Arrow schema comes from {@link ProtobufArrowSchemas#toArrowSchema} once, at
 * construction, so every batch — including an empty one — carries the same columns.
 * Nested messages become {@code Struct} columns, {@code repeated} fields
 * {@code List}, map fields {@code Map}, and {@code google.protobuf.Timestamp} a UTC
 * microsecond timestamp; sub-microsecond nanos are truncated on conversion. Fields
 * without presence write their default value, so a proto3 implicit scalar that was
 * never set and one set to its default are indistinguishable after a round trip.
 *
 * <p>Combine the bridge with a {@code ProtobufSerde} in a
 * {@link io.krabka.streams.columnar.RowCodec} to read a registry-framed topic, or
 * use {@link ProtobufBatchCodec}, which packages exactly that composition.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var bridge = ProtobufRowBridge.of(Order.getDefaultInstance());
 * try (var batch = bridge.rowsToBatch(List.of(order), allocator)) {
 *     List<Order> back = bridge.batchToRows(batch);
 * }
 * }</pre>
 *
 * @param <T> the message type the bridge converts
 */
public final class ProtobufRowBridge<T extends Message> implements RowBridge<T> {
    private final T defaultInstance;
    private final Descriptors.Descriptor descriptor;
    private final List<Field> fields;

    private ProtobufRowBridge(T defaultInstance) {
        this.defaultInstance = defaultInstance;
        this.descriptor = defaultInstance.getDescriptorForType();
        this.fields = ProtobufArrowSchemas.topLevelFields(descriptor);
    }

    /**
     * Creates a bridge for one message type.
     *
     * @param <T> the message type
     * @param defaultInstance the default instance of the message type, for example
     *     {@code Order.getDefaultInstance()}
     * @return a bridge that converts messages of the instance's type
     * @throws ColumnarException if the descriptor uses an unsupported shape
     */
    public static <T extends Message> ProtobufRowBridge<T> of(T defaultInstance) {
        Objects.requireNonNull(defaultInstance, "defaultInstance");
        return new ProtobufRowBridge<>(defaultInstance);
    }

    /**
     * Returns the Arrow schema every batch of this bridge uses.
     *
     * @return the Arrow schema derived from the message descriptor
     */
    public org.apache.arrow.vector.types.pojo.Schema arrowSchema() {
        return new org.apache.arrow.vector.types.pojo.Schema(fields);
    }

    /**
     * Builds one Arrow batch with one row per message.
     *
     * @param rows the messages to convert, in record order
     * @param allocator the allocator that owns the batch's buffers
     * @return the payload batch; the caller must close it
     * @throws ColumnarException if a value cannot be converted
     */
    @Override
    public VectorSchemaRoot rowsToBatch(List<T> rows, BufferAllocator allocator) {
        var root = ArrowValues.createRoot(fields, rows.size(), allocator);
        try {
            for (int column = 0; column < fields.size(); column++) {
                var field = fields.get(column);
                var descriptorField = descriptor.getFields().get(column);
                var vector = root.getVector(column);
                for (int row = 0; row < rows.size(); row++) {
                    ArrowValues.set(vector, row, columnValue(rows.get(row), descriptorField, field));
                }
            }
            ArrowValues.finish(root);
            return root;
        } catch (RuntimeException error) {
            root.close();
            throw error;
        }
    }

    /**
     * Converts an Arrow payload batch back into one message per row.
     *
     * @param batch the payload batch to convert; the bridge reads it and leaves it
     *     open
     * @return the messages in row order
     * @throws ColumnarException if a row cannot be converted back to the message
     *     type
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<T> batchToRows(VectorSchemaRoot batch) {
        var rows = new ArrayList<T>(batch.getRowCount());
        for (int row = 0; row < batch.getRowCount(); row++) {
            var builder = defaultInstance.newBuilderForType();
            for (int column = 0; column < fields.size(); column++) {
                var field = fields.get(column);
                var descriptorField = descriptor.getFields().get(column);
                var vector = batch.getVector(field.getName());
                if (vector == null) {
                    throw new ColumnarException("Arrow batch has no column " + field.getName());
                }
                setColumn(builder, descriptorField, field, ArrowValues.get(vector, row));
            }
            rows.add((T) builder.build());
        }
        return List.copyOf(rows);
    }

    private Object columnValue(Message message, Descriptors.FieldDescriptor field, Field arrowField) {
        if (field.isMapField()) {
            var keyField = field.getMessageType().findFieldByName("key");
            var valueField = field.getMessageType().findFieldByName("value");
            var entriesField = arrowField.getChildren().get(0);
            var valueArrow = entriesField.getChildren().get(1);
            var result = new LinkedHashMap<Object, Object>();
            for (var entry : (Collection<?>) message.getField(field)) {
                var entryMessage = (Message) entry;
                result.put(
                        scalarToArrow(entryMessage.getField(keyField), keyField, entriesField.getChildren().get(0)),
                        valueToArrow(entryMessage.getField(valueField), valueField, valueArrow));
            }
            return result;
        }
        if (field.isRepeated()) {
            var itemField = arrowField.getChildren().get(0);
            var result = new ArrayList<Object>();
            for (var item : (Collection<?>) message.getField(field)) {
                result.add(valueToArrow(item, field, itemField));
            }
            return result;
        }
        if (field.hasPresence() && !message.hasField(field)) {
            return null;
        }
        return valueToArrow(message.getField(field), field, arrowField);
    }

    private Object valueToArrow(Object value, Descriptors.FieldDescriptor field, Field arrowField) {
        if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                || field.getType() == Descriptors.FieldDescriptor.Type.GROUP) {
            return messageToArrow((Message) value, arrowField);
        }
        return scalarToArrow(value, field, arrowField);
    }

    private Object scalarToArrow(Object value, Descriptors.FieldDescriptor field, Field arrowField) {
        return switch (field.getType()) {
            case UINT32, FIXED32 -> Integer.toUnsignedLong((Integer) value);
            case UINT64, FIXED64 -> new BigInteger(Long.toUnsignedString((Long) value));
            case ENUM -> {
                var symbol = (Descriptors.EnumValueDescriptor) value;
                yield symbol.getIndex() < 0 ? Integer.toString(symbol.getNumber()) : symbol.getName();
            }
            case BYTES -> ((ByteString) value).toByteArray();
            case MESSAGE, GROUP -> throw new AssertionError("message handled by valueToArrow");
            default -> value;
        };
    }

    private Object messageToArrow(Message message, Field arrowField) {
        var metadata = arrowField.getMetadata();
        if ("true".equals(metadata.get(BridgeMetadata.JSON))) {
            try {
                return JsonFormat.printer().omittingInsignificantWhitespace().print(message);
            } catch (InvalidProtocolBufferException error) {
                throw new ColumnarException(
                        "cannot encode " + message.getDescriptorForType().getFullName()
                                + " as JSON in column " + arrowField.getName(),
                        error);
            }
        }
        if (metadata.containsKey(BridgeMetadata.PROTO_WRAPPER)) {
            var valueField = message.getDescriptorForType().findFieldByName("value");
            return scalarToArrow(message.getField(valueField), valueField, arrowField);
        }
        if (arrowField.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) {
            var descriptorType = message.getDescriptorForType();
            long seconds = (Long) message.getField(descriptorType.findFieldByName("seconds"));
            int nanos = (Integer) message.getField(descriptorType.findFieldByName("nanos"));
            return Math.addExact(Math.multiplyExact(seconds, 1_000_000L), nanos / 1_000L);
        }
        var result = new LinkedHashMap<String, Object>();
        var children = arrowField.getChildren();
        var messageFields = message.getDescriptorForType().getFields();
        for (int index = 0; index < children.size(); index++) {
            result.put(
                    children.get(index).getName(),
                    columnValue(message, messageFields.get(index), children.get(index)));
        }
        return result;
    }

    private void setColumn(
            Message.Builder builder, Descriptors.FieldDescriptor field, Field arrowField, Object value) {
        if (value == null) {
            return;
        }
        if (field.isMapField()) {
            var keyField = field.getMessageType().findFieldByName("key");
            var valueField = field.getMessageType().findFieldByName("value");
            var entriesField = arrowField.getChildren().get(0);
            for (var entry : (Collection<?>) value) {
                var pair = (Map<?, ?>) entry;
                var entryBuilder = builder.newBuilderForField(field);
                setSingular(entryBuilder, keyField, entriesField.getChildren().get(0), pair.get("key"));
                setSingular(entryBuilder, valueField, entriesField.getChildren().get(1), pair.get("value"));
                builder.addRepeatedField(field, entryBuilder.build());
            }
            return;
        }
        if (field.isRepeated()) {
            var itemField = arrowField.getChildren().get(0);
            for (var item : (Collection<?>) value) {
                if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                        || field.getType() == Descriptors.FieldDescriptor.Type.GROUP) {
                    var itemBuilder = builder.newBuilderForField(field);
                    populateMessage(itemBuilder, itemField, item);
                    builder.addRepeatedField(field, itemBuilder.build());
                } else {
                    builder.addRepeatedField(field, scalarFromArrow(item, field));
                }
            }
            return;
        }
        setSingular(builder, field, arrowField, value);
    }

    private void setSingular(
            Message.Builder builder, Descriptors.FieldDescriptor field, Field arrowField, Object value) {
        if (value == null) {
            return;
        }
        if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                || field.getType() == Descriptors.FieldDescriptor.Type.GROUP) {
            var nested = builder.newBuilderForField(field);
            populateMessage(nested, arrowField, value);
            builder.setField(field, nested.build());
            return;
        }
        builder.setField(field, scalarFromArrow(value, field));
    }

    private void populateMessage(Message.Builder builder, Field arrowField, Object value) {
        var metadata = arrowField.getMetadata();
        if ("true".equals(metadata.get(BridgeMetadata.JSON))) {
            try {
                JsonFormat.parser().merge(value.toString(), builder);
            } catch (InvalidProtocolBufferException error) {
                throw new ColumnarException(
                        "cannot parse " + metadata.get(BridgeMetadata.PROTO_MESSAGE)
                                + " JSON text in column " + arrowField.getName(),
                        error);
            }
            return;
        }
        if (metadata.containsKey(BridgeMetadata.PROTO_WRAPPER)) {
            var valueField = builder.getDescriptorForType().findFieldByName("value");
            builder.setField(valueField, scalarFromArrow(value, valueField));
            return;
        }
        if (arrowField.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) {
            long micros = ((Number) value).longValue();
            var descriptorType = builder.getDescriptorForType();
            builder.setField(descriptorType.findFieldByName("seconds"), Math.floorDiv(micros, 1_000_000L));
            builder.setField(
                    descriptorType.findFieldByName("nanos"),
                    Math.toIntExact(Math.floorMod(micros, 1_000_000L) * 1_000L));
            return;
        }
        var struct = (Map<?, ?>) value;
        var children = arrowField.getChildren();
        var messageFields = builder.getDescriptorForType().getFields();
        for (int index = 0; index < children.size(); index++) {
            setColumn(builder, messageFields.get(index), children.get(index), struct.get(children.get(index).getName()));
        }
    }

    private Object scalarFromArrow(Object value, Descriptors.FieldDescriptor field) {
        return switch (field.getType()) {
            case INT32, SINT32, SFIXED32 -> ((Number) value).intValue();
            case UINT32, FIXED32 -> (int) ((Number) value).longValue();
            case INT64, SINT64, SFIXED64 -> ((Number) value).longValue();
            case UINT64, FIXED64 -> ((Number) value).longValue();
            case FLOAT -> ((Number) value).floatValue();
            case DOUBLE -> ((Number) value).doubleValue();
            case BOOL -> value;
            case STRING -> value.toString();
            case BYTES -> ByteString.copyFrom(bytes(value));
            case ENUM -> enumValue(value.toString(), field.getEnumType());
            case MESSAGE, GROUP -> throw new AssertionError("message handled by callers");
        };
    }

    private static Descriptors.EnumValueDescriptor enumValue(String symbol, Descriptors.EnumDescriptor type) {
        if (!symbol.isEmpty() && (Character.isDigit(symbol.charAt(0)) || symbol.charAt(0) == '-')) {
            try {
                return type.findValueByNumberCreatingIfUnknown(Integer.parseInt(symbol));
            } catch (NumberFormatException ignored) {
                // fall through to name lookup
            }
        }
        var value = type.findValueByName(symbol);
        if (value == null) {
            throw new ColumnarException("enum " + type.getFullName() + " has no value " + symbol);
        }
        return value;
    }

    private static byte[] bytes(Object value) {
        if (value instanceof ByteBuffer buffer) {
            var copy = buffer.duplicate();
            var result = new byte[copy.remaining()];
            copy.get(result);
            return result;
        }
        return (byte[]) value;
    }
}
