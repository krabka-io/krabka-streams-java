package io.krabka.streams.columnar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;

/** A validated and reusable columnar topology. */
public final class BuiltColumnarTopology implements AutoCloseable {
    private final ColumnarTopology topology;
    private final Map<Integer, Map<Integer, ColumnarProcessor>> processors = new HashMap<>();

    BuiltColumnarTopology(ColumnarTopology topology) {
        this.topology = topology;
    }

    public synchronized List<ProducedToTopic> runBatch(String topic, List<ConsumedRecord> records) {
        return runBatches(Map.of(topic, records));
    }

    /** Runs records from multiple source topics as one graph evaluation, enabling fan-in. */
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

    /** Evaluates one co-partitioned set of source batches against isolated processor state. */
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

    /** Returns snapshots keyed by operator name for one logical partition. */
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

    /** Restores snapshots before processing the partition. Unknown operator names are ignored. */
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

    /** Drops all processor state owned by a logical partition. */
    public synchronized void releasePartition(int partition) {
        closeProcessors(processors.remove(partition));
    }

    synchronized boolean hasPartition(int partition) {
        return processors.containsKey(partition);
    }

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
