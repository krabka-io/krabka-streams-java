package io.krabka.streams.schema;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;

/** Encodes and decodes Confluent schema registry frames. */
public final class ConfluentWireFormat {
    public static final byte MAGIC = 0;
    private static final int HEADER_SIZE = 5;

    private ConfluentWireFormat() {
    }

    public static byte[] encode(int schemaId, byte[] body) {
        var result = ByteBuffer.allocate(HEADER_SIZE + body.length);
        result.put(MAGIC).putInt(schemaId).put(body);
        return result.array();
    }

    public static Frame decode(byte[] bytes) {
        var buffer = header(bytes);
        int schemaId = buffer.getInt(1);
        buffer.position(HEADER_SIZE);
        var body = new byte[buffer.remaining()];
        buffer.get(body);
        return new Frame(schemaId, body);
    }

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

    public record Frame(int schemaId, byte[] body) {
        public Frame {
            body = body.clone();
        }

        @Override
        public byte[] body() {
            return body.clone();
        }
    }

    public record ProtobufFrame(int schemaId, List<Integer> messageIndexes, byte[] body) {
        public ProtobufFrame {
            messageIndexes = List.copyOf(messageIndexes);
            body = body.clone();
        }

        @Override
        public byte[] body() {
            return body.clone();
        }
    }
}
