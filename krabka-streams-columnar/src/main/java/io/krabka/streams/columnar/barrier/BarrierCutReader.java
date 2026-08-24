package io.krabka.streams.columnar.barrier;

import io.krabka.streams.columnar.ColumnarException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Reads published cuts from the internal {@code __barrier_state} topic.
 *
 * <p>The reader drives a caller-owned consumer with {@code assign}, {@code seek}, and
 * {@code poll}, so the read needs no consumer group and no new broker RPC. Give it a
 * consumer of its own: the first read replaces the consumer's assignment with every
 * partition of {@code __barrier_state} and seeks to the beginning. Later reads
 * continue from the position the last read left, so a read costs one poll of the
 * records the coordinator published since.
 *
 * <p>The reader keeps complete cuts only. A partial cut names partitions that never
 * received the epoch's marker, so a task that waited for one would wait forever. That
 * is why the coordinator publishes partial cuts at all: a reader can skip the epoch
 * instead of stalling on markers that never arrive.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var reader = new BarrierCutReader(cutConsumer);
 * Optional<BarrierCut> latest = reader.latestCompleteCut("transactions");
 * for (var cut : reader.completeCutsAfter("transactions", latest.get().epoch())) {
 *     System.out.println(cut.epoch());
 * }
 * }</pre>
 */
public final class BarrierCutReader {
    /** The poll timeout a reader uses when the caller names none. */
    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(1);

    private final Consumer<byte[], byte[]> consumer;
    private final Duration pollTimeout;
    private final Map<String, TreeMap<Long, BarrierCut>> cuts = new HashMap<>();
    private List<TopicPartition> assignment;

    /**
     * Creates a reader over a consumer, with the default poll timeout.
     *
     * @param consumer the consumer the reader assigns, seeks, and polls
     */
    public BarrierCutReader(Consumer<byte[], byte[]> consumer) {
        this(consumer, DEFAULT_POLL_TIMEOUT);
    }

    /**
     * Creates a reader over a consumer.
     *
     * @param consumer the consumer the reader assigns, seeks, and polls
     * @param pollTimeout how long one poll of {@code __barrier_state} may block
     */
    public BarrierCutReader(Consumer<byte[], byte[]> consumer, Duration pollTimeout) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
    }

    /**
     * Returns the newest complete cut of a barrier group.
     *
     * @param group the barrier group to read
     * @return the complete cut with the highest epoch, or empty when the group has
     *     none
     * @throws ColumnarException if {@code __barrier_state} is absent or a record is
     *     malformed
     */
    public Optional<BarrierCut> latestCompleteCut(String group) {
        refresh();
        var byEpoch = cuts.get(Objects.requireNonNull(group, "group"));
        return byEpoch == null || byEpoch.isEmpty()
                ? Optional.empty()
                : Optional.of(byEpoch.lastEntry().getValue());
    }

    /**
     * Returns the complete cuts of a barrier group above an epoch.
     *
     * @param group the barrier group to read
     * @param epoch the exclusive lower bound; pass {@code -1} for every cut
     * @return the matching cuts in ascending epoch order
     * @throws ColumnarException if {@code __barrier_state} is absent or a record is
     *     malformed
     */
    public List<BarrierCut> completeCutsAfter(String group, long epoch) {
        refresh();
        var byEpoch = cuts.get(Objects.requireNonNull(group, "group"));
        return byEpoch == null ? List.of() : List.copyOf(byEpoch.tailMap(epoch, false).values());
    }

    private void refresh() {
        if (assignment == null) {
            var partitions = consumer.partitionsFor(BarrierCutDecoder.TOPIC);
            if (partitions == null || partitions.isEmpty()) {
                throw new ColumnarException("barrier state topic " + BarrierCutDecoder.TOPIC + " has no partitions");
            }
            assignment = partitions.stream()
                    .map(partition -> new TopicPartition(BarrierCutDecoder.TOPIC, partition.partition()))
                    .toList();
            consumer.assign(assignment);
            consumer.seekToBeginning(assignment);
        }
        while (behind()) {
            var polled = consumer.poll(pollTimeout);
            if (polled.isEmpty()) {
                return;
            }
            polled.forEach(record -> BarrierCutDecoder.decode(record.key(), record.value())
                    .filter(BarrierCut::complete)
                    .ifPresent(cut -> cuts.computeIfAbsent(cut.group(), ignored -> new TreeMap<>())
                            .put(cut.epoch(), cut)));
        }
    }

    private boolean behind() {
        var ends = consumer.endOffsets(assignment);
        return assignment.stream()
                .anyMatch(partition -> consumer.position(partition) < ends.getOrDefault(partition, 0L));
    }
}
