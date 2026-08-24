package io.krabka.streams.columnar.barrier;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.common.TopicPartition;

/** Builds {@code __barrier_state} records in the frozen wire format. */
public final class BarrierTestRecords {
    /** The key kind of a group definition record. */
    public static final int GROUP_KIND = 0;

    /** The key kind of an injection-start record. */
    public static final int INJECTION_START_KIND = 1;

    /** The key kind of a cut record. */
    public static final int CUT_KIND = 2;

    private BarrierTestRecords() {
    }

    /**
     * Encodes a {@code __barrier_state} key.
     *
     * @param kind the record kind
     * @param group the barrier group name
     * @param epoch the epoch, or {@code -1} for a group record
     * @return the key bytes
     */
    public static byte[] key(int kind, String group, long epoch) {
        return write(output -> {
            output.writeShort(0);
            output.writeShort(kind);
            string(output, group);
            output.writeLong(epoch);
        });
    }

    /**
     * Encodes a cut value.
     *
     * @param triggeredAt the injection start time in epoch milliseconds
     * @param completedAt the publication time in epoch milliseconds
     * @param status whether every partition received the marker
     * @param offsets the marker offset of every partition that received one
     * @param missing the partitions that received no marker
     * @return the value bytes
     */
    public static byte[] cutValue(
            long triggeredAt,
            long completedAt,
            BarrierCutStatus status,
            Map<TopicPartition, Long> offsets,
            Collection<TopicPartition> missing) {
        var byTopic = new TreeMap<String, TreeMap<Integer, Long>>();
        offsets.forEach((partition, offset) -> byTopic
                .computeIfAbsent(partition.topic(), ignored -> new TreeMap<>())
                .put(partition.partition(), offset));
        var absent = List.copyOf(missing);
        return write(output -> {
            output.writeShort(0);
            output.writeLong(triggeredAt);
            output.writeLong(completedAt);
            output.writeByte(status.code());
            output.writeInt(byTopic.size());
            for (var topic : byTopic.entrySet()) {
                string(output, topic.getKey());
                output.writeInt(topic.getValue().size());
                for (var partition : topic.getValue().entrySet()) {
                    output.writeInt(partition.getKey());
                    output.writeLong(partition.getValue());
                }
            }
            output.writeInt(absent.size());
            for (var partition : absent) {
                string(output, partition.topic());
                output.writeInt(partition.partition());
            }
        });
    }

    private static void string(DataOutputStream output, String value) throws IOException {
        var bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeShort(bytes.length);
        output.write(bytes);
    }

    private static byte[] write(Body body) {
        var bytes = new ByteArrayOutputStream();
        try (var output = new DataOutputStream(bytes)) {
            body.writeTo(output);
        } catch (IOException error) {
            throw new UncheckedIOException(error);
        }
        return bytes.toByteArray();
    }

    private interface Body {
        void writeTo(DataOutputStream output) throws IOException;
    }
}
