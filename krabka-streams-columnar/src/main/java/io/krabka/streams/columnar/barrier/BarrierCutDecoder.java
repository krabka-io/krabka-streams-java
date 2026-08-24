package io.krabka.streams.columnar.barrier;

import io.krabka.streams.columnar.ColumnarException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;

/**
 * Decodes the cut records of the internal {@code __barrier_state} topic.
 *
 * <p>The topic carries three record kinds and the key discriminates them: a group
 * definition, an injection start, and a cut. Only the cut kind matters to a client, so
 * the decoder returns an empty result for the other two and for a tombstone. Every
 * integer is big-endian and a string is an {@code i16} byte length and then UTF-8
 * bytes, exactly as the barrier design freezes the format for all three krabka streams
 * libraries.
 *
 * <p>Malformed bytes raise {@link ColumnarException}. The decoder never returns a
 * partly filled cut.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * for (var record : consumer.poll(Duration.ofSeconds(1))) {
 *     BarrierCutDecoder.decode(record.key(), record.value())
 *         .filter(BarrierCut::complete)
 *         .ifPresent(cut -> System.out.println(cut.epoch() + " " + cut.offsets()));
 * }
 * }</pre>
 */
public final class BarrierCutDecoder {
    /** The internal topic the coordinator publishes barrier state to. */
    public static final String TOPIC = "__barrier_state";

    private static final int RECORD_VERSION = 0;
    private static final int CUT_KIND = 2;

    private BarrierCutDecoder() {
    }

    /**
     * Decodes one {@code __barrier_state} record into a cut.
     *
     * @param key the record key bytes
     * @param value the record value bytes, or null for a tombstone
     * @return the cut, or empty when the record is not a cut
     * @throws ColumnarException if the key or the value is malformed
     */
    public static Optional<BarrierCut> decode(byte[] key, byte[] value) {
        if (key == null) {
            return Optional.empty();
        }
        var keyBuffer = ByteBuffer.wrap(key.clone());
        try {
            short version = keyBuffer.getShort();
            if (version != RECORD_VERSION) {
                throw new ColumnarException("unsupported barrier record version " + version);
            }
            short kind = keyBuffer.getShort();
            var group = string(keyBuffer);
            long epoch = keyBuffer.getLong();
            if (kind != CUT_KIND) {
                return Optional.empty();
            }
            if (keyBuffer.hasRemaining()) {
                throw new ColumnarException("trailing bytes in barrier cut key");
            }
            if (value == null) {
                return Optional.empty();
            }
            return Optional.of(cut(group, epoch, ByteBuffer.wrap(value.clone())));
        } catch (BufferUnderflowException error) {
            throw new ColumnarException("truncated barrier cut record", error);
        }
    }

    private static BarrierCut cut(String group, long epoch, ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version != RECORD_VERSION) {
            throw new ColumnarException("unsupported barrier cut version " + version);
        }
        long triggeredAt = buffer.getLong();
        long completedAt = buffer.getLong();
        var status = BarrierCutStatus.fromCode(buffer.get());
        var offsets = new HashMap<TopicPartition, Long>();
        int topics = count(buffer, "topic");
        for (int topic = 0; topic < topics; topic++) {
            var name = string(buffer);
            int partitions = count(buffer, "partition");
            for (int index = 0; index < partitions; index++) {
                int partition = buffer.getInt();
                offsets.put(new TopicPartition(name, partition), buffer.getLong());
            }
        }
        var missing = new HashSet<TopicPartition>();
        int absent = count(buffer, "missing partition");
        for (int index = 0; index < absent; index++) {
            missing.add(new TopicPartition(string(buffer), buffer.getInt()));
        }
        if (buffer.hasRemaining()) {
            throw new ColumnarException("trailing bytes in barrier cut record");
        }
        return new BarrierCut(group, epoch, triggeredAt, completedAt, status, offsets, missing);
    }

    private static int count(ByteBuffer buffer, String element) {
        int count = buffer.getInt();
        if (count < 0) {
            throw new ColumnarException("negative barrier cut " + element + " count");
        }
        if (count > buffer.remaining()) {
            throw new ColumnarException("truncated barrier cut record");
        }
        return count;
    }

    private static String string(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 0) {
            throw new ColumnarException("negative barrier string length " + length);
        }
        if (length > buffer.remaining()) {
            throw new ColumnarException("truncated barrier cut record");
        }
        var bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
