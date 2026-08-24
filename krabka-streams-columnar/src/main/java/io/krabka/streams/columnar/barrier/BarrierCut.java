package io.krabka.streams.columnar.barrier;

import io.krabka.streams.columnar.ConsumedRecord;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

/**
 * One epoch's cut across every partition of a barrier group.
 *
 * <p>A cut names an exact position in every input at once: the offset of the epoch's
 * marker in each partition. Records below that offset are before the cut and records
 * at or above it are after the cut. The marker itself is a Kafka control record, so a
 * {@code KafkaConsumer} never delivers it and the cut offset never holds a data
 * record. "Everything before the cut" is exactly {@code offset < cutOffset}.
 *
 * <p>{@link BarrierCutReader} decodes cuts from the internal {@code __barrier_state}
 * topic. The offsets and the missing partitions are copied on construction, so a
 * decoded cut is immutable and safe to share between threads.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var partition = new TopicPartition("transactions", 0);
 * List<ConsumedRecord> beforeTheCut = cut.recordsBefore(partition, polled);
 * boolean waitingOnThisPartition = !cut.reached(partition, consumer.position(partition));
 * }</pre>
 *
 * @param group the barrier group the coordinator injected markers for
 * @param epoch the epoch that identifies this cut within the group
 * @param triggeredAt the time the injection started, in epoch milliseconds
 * @param completedAt the time the coordinator published the cut, in epoch milliseconds
 * @param status whether every partition received the epoch's marker
 * @param offsets the marker offset of every partition that received one
 * @param missing the partitions that never received the epoch's marker
 */
public record BarrierCut(
        String group,
        long epoch,
        long triggeredAt,
        long completedAt,
        BarrierCutStatus status,
        Map<TopicPartition, Long> offsets,
        Set<TopicPartition> missing) {
    /**
     * Copies the mutable components so the cut is immutable.
     *
     * @param group the barrier group the coordinator injected markers for
     * @param epoch the epoch that identifies this cut within the group
     * @param triggeredAt the time the injection started, in epoch milliseconds
     * @param completedAt the time the coordinator published the cut, in epoch
     *     milliseconds
     * @param status whether every partition received the epoch's marker
     * @param offsets the marker offset of every partition that received one
     * @param missing the partitions that never received the epoch's marker
     */
    public BarrierCut {
        java.util.Objects.requireNonNull(group, "group");
        java.util.Objects.requireNonNull(status, "status");
        offsets = Map.copyOf(offsets);
        missing = Set.copyOf(missing);
    }

    /**
     * Reports whether every partition of the group received the epoch's marker.
     *
     * @return true when the status is {@link BarrierCutStatus#COMPLETE}
     */
    public boolean complete() {
        return status == BarrierCutStatus.COMPLETE;
    }

    /**
     * Returns a partition's marker offset.
     *
     * @param partition the topic partition to look up
     * @return the marker offset, or empty when the partition is not in this cut
     */
    public OptionalLong offset(TopicPartition partition) {
        var offset = offsets.get(partition);
        return offset == null ? OptionalLong.empty() : OptionalLong.of(offset);
    }

    /**
     * Returns the records of a partition that fall before the cut.
     *
     * <p>A partition that this cut does not name is not part of the barrier group, so
     * every record passes through unchanged.
     *
     * @param partition the topic partition the records were fetched from
     * @param records the fetched records, in offset order
     * @return the prefix whose offsets are below the partition's marker offset
     */
    public List<ConsumedRecord> recordsBefore(TopicPartition partition, List<ConsumedRecord> records) {
        var offset = offsets.get(partition);
        if (offset == null) {
            return List.copyOf(records);
        }
        return records.stream().filter(record -> record.offset() < offset).toList();
    }

    /**
     * Reports whether a partition delivered everything below the cut.
     *
     * <p>The position is the consumer's next fetch offset. A partition that this cut
     * does not name holds nothing back and always reports true.
     *
     * @param partition the topic partition to test
     * @param position the consumer's current position in that partition
     * @return true when nothing before the cut is still unread
     */
    public boolean reached(TopicPartition partition, long position) {
        var offset = offsets.get(partition);
        return offset == null || position >= offset;
    }
}
