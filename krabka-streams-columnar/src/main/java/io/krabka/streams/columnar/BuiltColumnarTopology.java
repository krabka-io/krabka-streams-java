package io.krabka.streams.columnar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * A validated and reusable columnar topology.
 *
 * <p>Created by {@link ColumnarTopology#build()}. The built form owns the processor
 * instances, one set per logical partition number, created lazily when a partition
 * first processes records. Stateful operators therefore accumulate across calls: keep
 * one built topology for the lifetime of a running partition or consumer group
 * member, and {@link #close()} it to release processor state.
 *
 * <p>All methods are synchronized; one built topology processes one batch at a time.
 * Every intermediate Arrow batch created during an evaluation is closed before the
 * method returns, so callers only handle the returned byte-backed records.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try (var built = topology.build()) {
 *     List<ProducedToTopic> output = built.runBatch("transactions", records);
 *
 *     Map<String, byte[]> snapshots = built.snapshotPartition(0);
 *     built.releasePartition(0);
 *     built.restorePartition(0, snapshots);
 * }
 * }</pre>
 */
public final class BuiltColumnarTopology implements AutoCloseable {
    private final ColumnarTopology topology;
    private final Map<Integer, Map<Integer, ColumnarProcessor>> processors = new HashMap<>();

    BuiltColumnarTopology(ColumnarTopology topology) {
        this.topology = topology;
    }

    /**
     * Runs one source topic's records through the graph.
     *
     * <p>Records are grouped by their partition number and each partition is
     * evaluated against its own processor state, in ascending partition order.
     *
     * @param topic the source topic the records belong to
     * @param records the fetched records
     * @return everything the topology's sinks produced, in evaluation order
     */
    public synchronized List<ProducedToTopic> runBatch(String topic, List<ConsumedRecord> records) {
        return runBatches(Map.of(topic, records));
    }

    /**
     * Runs records from multiple source topics as one graph evaluation, enabling
     * fan-in.
     *
     * <p>Use this form when a join or merge needs both sides' records in the same
     * evaluation. Records are grouped by partition number across all topics, so
     * co-partitioned topics join partition by partition.
     *
     * @param input source topic to fetched records
     * @return everything the topology's sinks produced, in evaluation order
     */
    public synchronized List<ProducedToTopic> runBatches(Map<String, List<ConsumedRecord>> input) {
        var partitions = new java.util.TreeSet<Integer>();
        input.values().forEach(records -> records.forEach(record -> partitions.add(record.partition())));
        var result = new ArrayList<ProducedToTopic>();
        for (int partition : partitions) {
            var partitionInput = new HashMap<String, List<ConsumedRecord>>();
            input.forEach((topic, records) -> partitionInput.put(
                    topic,
                    records.stream().filter(record -> record.partition() == partition).toList()));
            result.addAll(runPartitionBatches(partition, partitionInput));
        }
        return List.copyOf(result);
    }

    /**
     * Evaluates one co-partitioned set of source batches against isolated processor
     * state.
     *
     * <p>This is the lowest-level entry point, used by the runners after they have
     * grouped a poll by partition. All records must carry the given partition number.
     * If the evaluation throws, partially forwarded batches are closed, but operator
     * state may have advanced; runners roll the partition back with
     * {@link #restorePartition(int, Map)} in that case.
     *
     * @param partition the logical partition whose processor state is used
     * @param input source topic to that partition's fetched records
     * @return everything the topology's sinks produced, in evaluation order
     * @throws IllegalArgumentException if a record belongs to another partition
     * @throws ColumnarException if decoding, an operator, or encoding fails
     */
    public synchronized List<ProducedToTopic> runPartitionBatches(
            int partition, Map<String, List<ConsumedRecord>> input) {
        input.values().stream().flatMap(List::stream).forEach(record -> {
            if (record.partition() != partition) {
                throw new IllegalArgumentException("record does not belong to partition " + partition);
            }
        });
        Map<Integer, List<VectorSchemaRoot>> frames = new HashMap<>();
        var produced = new ArrayList<ProducedToTopic>();
        try {
            var nodes = topology.nodes();
            for (int index = 0; index < nodes.size(); index++) {
                var node = nodes.get(index);
                switch (node.type()) {
                    case SOURCE -> {
                        int sourceIndex = index;
                        boolean needsDecodedFrame = nodes.stream().anyMatch(candidate ->
                                candidate.parents().stream().anyMatch(parent -> parent.index() == sourceIndex)
                                        && (candidate.type() != ColumnarTopology.NodeType.SINK
                                                || candidate.sinkCodec() != null));
                        var decoded = new ArrayList<VectorSchemaRoot>();
                        if (needsDecodedFrame) {
                            input.forEach((topic, records) -> {
                                if (!records.isEmpty() && node.sourceTopics().contains(topic)) {
                                    decoded.add(node.sourceCodec().decode(topic, records));
                                }
                            });
                        }
                        frames.put(index, List.copyOf(decoded));
                    }
                    case OPERATOR -> frames.put(index, runOperator(partition, index, node, frames));
                    case MERGE -> frames.put(index, merge(node, frames));
                    case JOIN -> frames.put(index, runJoin(partition, index, node, frames));
                    case SINK -> {
                        if (node.sinkCodec() == null) {
                            var parent = nodes.get(node.parents().get(0).index());
                            input.forEach((topic, records) -> {
                                if (parent.sourceTopics().contains(topic)) {
                                    records.forEach(record -> produced.add(new ProducedToTopic(
                                            node.sinkTopic(),
                                            new ProduceRecord(
                                                    record.key(),
                                                    record.value(),
                                                    record.timestamp(),
                                                    record.headers()))));
                                }
                            });
                            break;
                        }
                        for (var batch : frames.getOrDefault(node.parents().get(0).index(), List.of())) {
                            for (var record : node.sinkCodec().encode(node.sinkTopic(), batch)) {
                                produced.add(new ProducedToTopic(node.sinkTopic(), record));
                            }
                        }
                    }
                    default -> throw new IllegalStateException("unknown columnar node type " + node.type());
                }
            }
            return List.copyOf(produced);
        } finally {
            var closed = Collections.newSetFromMap(new IdentityHashMap<VectorSchemaRoot, Boolean>());
            frames.values().stream().flatMap(List::stream).forEach(batch -> {
                if (closed.add(batch)) {
                    batch.close();
                }
            });
        }
    }

    private List<VectorSchemaRoot> runOperator(
            int partition,
            int nodeIndex,
            ColumnarTopology.NodeDefinition node,
            Map<Integer, List<VectorSchemaRoot>> frames) {
        var outputs = new ArrayList<VectorSchemaRoot>();
        var processor = processors(partition).get(nodeIndex);
        for (var parent : frames.getOrDefault(node.parents().get(0).index(), List.of())) {
            var input = ArrowBatchSupport.copyRange(
                    parent, 0, parent.getRowCount(), topology.allocator());
            var context = new ColumnarContext();
            try {
                processor.process(context, input);
                boolean forwardedInput = context.contains(input);
                outputs.addAll(context.drain());
                if (!forwardedInput) {
                    input.close();
                }
            } catch (RuntimeException error) {
                var toClose = Collections.newSetFromMap(new IdentityHashMap<VectorSchemaRoot, Boolean>());
                toClose.add(input);
                toClose.addAll(context.drain());
                toClose.addAll(outputs);
                toClose.forEach(VectorSchemaRoot::close);
                throw error;
            }
        }
        return List.copyOf(outputs);
    }

    /**
     * Returns snapshots keyed by operator name for one logical partition.
     *
     * <p>Only {@link StatefulColumnarProcessor} nodes, including joins, appear in the
     * result. A partition that has never processed records returns an empty map.
     *
     * @param partition the logical partition to snapshot
     * @return operator name to snapshot bytes; empty when the partition has no state
     */
    public synchronized Map<String, byte[]> snapshotPartition(int partition) {
        var partitionProcessors = processors.get(partition);
        if (partitionProcessors == null) {
            return Map.of();
        }
        var snapshots = new java.util.LinkedHashMap<String, byte[]>();
        var nodes = topology.nodes();
        partitionProcessors.forEach((index, processor) -> {
            if (processor instanceof StatefulColumnarProcessor stateful) {
                snapshots.put(nodes.get(index).name(), stateful.snapshot().clone());
            }
        });
        return java.util.Collections.unmodifiableMap(snapshots);
    }

    /**
     * Restores snapshots before processing the partition. Unknown operator names are
     * ignored.
     *
     * <p>Restoring creates the partition's processors if needed and replaces the
     * state of every stateful operator whose name appears in the map. Snapshots are
     * matched by node name, which is why names should stay stable across application
     * versions.
     *
     * @param partition the logical partition to restore
     * @param snapshots operator name to snapshot bytes, as returned by
     *     {@link #snapshotPartition(int)}
     * @throws ColumnarException if a named operator cannot restore its bytes
     */
    public synchronized void restorePartition(int partition, Map<String, byte[]> snapshots) {
        var byName = new HashMap<String, ColumnarProcessor>();
        var nodes = topology.nodes();
        processors(partition).forEach((index, processor) -> byName.put(nodes.get(index).name(), processor));
        snapshots.forEach((name, snapshot) -> {
            var processor = byName.get(name);
            if (processor instanceof StatefulColumnarProcessor stateful) {
                stateful.restore(snapshot.clone());
            }
        });
    }

    /**
     * Drops all processor state owned by a logical partition.
     *
     * <p>Closeable processors are closed. The next batch for the partition starts
     * from fresh processor instances.
     *
     * @param partition the logical partition to release
     * @throws ColumnarException if a processor fails to close
     */
    public synchronized void releasePartition(int partition) {
        closeProcessors(processors.remove(partition));
    }

    synchronized boolean hasPartition(int partition) {
        return processors.containsKey(partition);
    }

    /**
     * Releases every partition's processor state.
     *
     * <p>The underlying {@link ColumnarTopology} and allocator are not touched and
     * can build again.
     *
     * @throws ColumnarException if a processor fails to close
     */
    @Override
    public synchronized void close() {
        processors.values().forEach(BuiltColumnarTopology::closeProcessors);
        processors.clear();
    }

    private Map<Integer, ColumnarProcessor> processors(int partition) {
        return processors.computeIfAbsent(partition, ignored -> {
            var result = new HashMap<Integer, ColumnarProcessor>();
            var nodes = topology.nodes();
            for (int index = 0; index < nodes.size(); index++) {
                if (nodes.get(index).type() == ColumnarTopology.NodeType.OPERATOR) {
                    result.put(index, nodes.get(index).processor().get());
                } else if (nodes.get(index).type() == ColumnarTopology.NodeType.JOIN) {
                    result.put(index, new StatefulJoinProcessor(nodes.get(index).join(), topology.allocator()));
                }
            }
            return result;
        });
    }

    private static void closeProcessors(Map<Integer, ColumnarProcessor> partitionProcessors) {
        if (partitionProcessors == null) {
            return;
        }
        partitionProcessors.values().forEach(processor -> {
            if (processor instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception error) {
                    throw new ColumnarException("cannot close columnar processor", error);
                }
            }
        });
    }

    private List<VectorSchemaRoot> merge(
            ColumnarTopology.NodeDefinition node,
            Map<Integer, List<VectorSchemaRoot>> frames) {
        var inputs = node.parents().stream()
                .flatMap(parent -> frames.getOrDefault(parent.index(), List.of()).stream())
                .toList();
        if (inputs.isEmpty()) {
            return List.of();
        }
        return List.of(ArrowBatchSupport.concatenate(inputs, topology.allocator()));
    }

    private List<VectorSchemaRoot> runJoin(
            int partition,
            int nodeIndex,
            ColumnarTopology.NodeDefinition node,
            Map<Integer, List<VectorSchemaRoot>> frames) {
        var join = (StatefulJoinProcessor) processors(partition).get(nodeIndex);
        return join.process(
                frames.getOrDefault(node.parents().get(0).index(), List.of()),
                frames.getOrDefault(node.parents().get(1).index(), List.of()));
    }
}
