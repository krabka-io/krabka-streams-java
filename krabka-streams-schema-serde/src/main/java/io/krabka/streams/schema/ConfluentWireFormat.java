package io.krabka.streams.schema;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;

/**
 * Encodes and decodes Confluent schema registry frames.
 *
 * <p>Every framed record starts with a five-byte header: the {@link #MAGIC} byte
 * followed by the schema ID as a big-endian 32-bit integer. Protobuf records insert a
 * zigzag-varint message-index list between the header and the body; the common case of
 * the first top-level message is encoded as the single byte {@code 0}.
 *
 * <p>The serdes in this package frame records automatically. Use this class directly
 * when interoperating with foreign framed bytes, for example when inspecting records
 * in tooling or writing a custom serde.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * byte[] framed = ConfluentWireFormat.encode(42, body);
 * var frame = ConfluentWireFormat.decode(framed);
 * assert frame.schemaId() == 42;
 * assert Arrays.equals(frame.body(), body);
 * }</pre>
 */
public final class ConfluentWireFormat {
    /** The first byte of every Confluent-framed record. */
    public static final byte MAGIC = 0;

    private static final int HEADER_SIZE = 5;

    private ConfluentWireFormat() {
    }

    /**
     * Frames body bytes with the magic byte and schema ID.
     *
     * @param schemaId the registry schema ID to record in the header
     * @param body the encoded record body
     * @return the framed record bytes
     */
    public static byte[] encode(int schemaId, byte[] body) {
        var result = ByteBuffer.allocate(HEADER_SIZE + body.length);
        result.put(MAGIC).putInt(schemaId).put(body);
        return result.array();
    }

    /**
     * Splits framed record bytes into the schema ID and body.
     *
     * @param bytes the framed record bytes
     * @return the decoded frame
     * @throws SerializationException if the input is shorter than five bytes or does
     *     not start with {@link #MAGIC}
     */
    public static Frame decode(byte[] bytes) {
        var buffer = header(bytes);
        int schemaId = buffer.getInt(1);
        buffer.position(HEADER_SIZE);
        var body = new byte[buffer.remaining()];
        buffer.get(body);
        return new Frame(schemaId, body);
    }

    /**
     * Frames Protobuf body bytes, including the message-index list.
     *
     * <p>The indexes identify the message within its {@code .proto} file: each entry
     * is the message's position at its nesting level, outermost first. The common
     * {@code [0]} (first top-level message) is written in its compact single-byte
     * form.
     *
     * @param schemaId the registry schema ID to record in the header
     * @param messageIndexes the non-empty message-index path, outermost first
     * @param body the serialized Protobuf message
     * @return the framed record bytes
     * @throws IllegalArgumentException if {@code messageIndexes} is empty
     */
    public static byte[] encodeProtobuf(int schemaId, List<Integer> messageIndexes, byte[] body) {
        if (messageIndexes.isEmpty()) {
            throw new IllegalArgumentException("messageIndexes must not be empty");
        }
        var result = new ByteArrayOutputStream(HEADER_SIZE + body.length + 8);
        result.write(MAGIC);
        result.writeBytes(ByteBuffer.allocate(4).putInt(schemaId).array());
        if (messageIndexes.equals(List.of(0))) {
            result.write(0);
        } else {
            writeVarint(result, messageIndexes.size());
            messageIndexes.forEach(index -> writeVarint(result, index));
        }
        result.writeBytes(body);
        return result.toByteArray();
    }

    /**
     * Splits framed Protobuf record bytes into schema ID, message indexes, and body.
     *
     * @param bytes the framed record bytes
     * @return the decoded Protobuf frame
     * @throws SerializationException if the input is shorter than five bytes, does not
     *     start with {@link #MAGIC}, or carries a malformed message-index list
     */
    public static ProtobufFrame decodeProtobuf(byte[] bytes) {
        var buffer = header(bytes);
        int schemaId = buffer.getInt(1);
        buffer.position(HEADER_SIZE);
        long count = readVarint(buffer);
        List<Integer> indexes;
        if (count == 0) {
            indexes = List.of(0);
        } else {
            if (count < 0 || count > Integer.MAX_VALUE) {
                throw new SerializationException("invalid Protobuf message-index count: " + count);
            }
            var mutableIndexes = new ArrayList<Integer>((int) count);
            for (int index = 0; index < count; index++) {
                long value = readVarint(buffer);
                if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                    throw new SerializationException("Protobuf message index is outside the int range");
                }
                mutableIndexes.add((int) value);
            }
            indexes = List.copyOf(mutableIndexes);
        }
        var body = new byte[buffer.remaining()];
        buffer.get(body);
        return new ProtobufFrame(schemaId, indexes, body);
    }

    private static ByteBuffer header(byte[] bytes) {
        if (bytes == null || bytes.length < HEADER_SIZE) {
            throw new SerializationException("schema frame is shorter than 5 bytes");
        }
        if (bytes[0] != MAGIC) {
            throw new SerializationException(
                    "invalid schema frame magic byte 0x" + String.format("%02x", bytes[0]));
        }
        return ByteBuffer.wrap(bytes);
    }

    private static void writeVarint(ByteArrayOutputStream output, long value) {
        long encoded = (value << 1) ^ (value >> 63);
        while ((encoded & ~0x7fL) != 0) {
            output.write((int) ((encoded & 0x7f) | 0x80));
            encoded >>>= 7;
        }
        output.write((int) encoded);
    }

    private static long readVarint(ByteBuffer buffer) {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            if (!buffer.hasRemaining()) {
                throw new SerializationException("truncated Protobuf message-index varint");
            }
            int current = Byte.toUnsignedInt(buffer.get());
            result |= (long) (current & 0x7f) << shift;
            if ((current & 0x80) == 0) {
                return (result >>> 1) ^ -(result & 1);
            }
        }
        throw new SerializationException("Protobuf message-index varint is too long");
    }

    /**
     * A decoded frame. The body is defensively copied in both directions.
     *
     * @param schemaId the registry schema ID from the header
     * @param body the record body that followed the header
     */
    public record Frame(int schemaId, byte[] body) {
        /**
         * Copies the body so later mutation of the input array cannot change the frame.
         *
         * @param schemaId the registry schema ID from the header
         * @param body the record body that followed the header
         */
        public Frame {
            body = body.clone();
        }

        /**
         * Returns a copy of the record body.
         *
         * @return a fresh copy of the body bytes
         */
        @Override
        public byte[] body() {
            return body.clone();
        }
    }

    /**
     * A decoded Protobuf frame. The body is defensively copied in both directions.
     *
     * @param schemaId the registry schema ID from the header
     * @param messageIndexes the message-index path, outermost first
     * @param body the serialized Protobuf message that followed the indexes
     */
    public record ProtobufFrame(int schemaId, List<Integer> messageIndexes, byte[] body) {
        /**
         * Copies the mutable components so the frame is immutable.
         *
         * @param schemaId the registry schema ID from the header
         * @param messageIndexes the message-index path, outermost first
         * @param body the serialized Protobuf message that followed the indexes
         */
        public ProtobufFrame {
            messageIndexes = List.copyOf(messageIndexes);
            body = body.clone();
        }

        /**
         * Returns a copy of the serialized Protobuf message.
         *
         * @return a fresh copy of the body bytes
         */
        @Override
        public byte[] body() {
            return body.clone();
        }
    }
}
