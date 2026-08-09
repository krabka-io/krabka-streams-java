package io.krabka.streams.columnar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;

/** A validated and reusable columnar topology. */
public final class BuiltColumnarTopology {
    private final ColumnarTopology topology;
    private final Map<Integer, ColumnarProcessor> processors = new HashMap<>();

    BuiltColumnarTopology(ColumnarTopology topology) {
        this.topology = topology;
        var nodes = topology.nodes();
        for (int index = 0; index < nodes.size(); index++) {
            if (nodes.get(index).type() == ColumnarTopology.NodeType.OPERATOR) {
                processors.put(index, nodes.get(index).processor().get());
            }
        }
    }

    public synchronized List<ProducedToTopic> runBatch(String topic, List<ConsumedRecord> records) {
        return runBatches(Map.of(topic, records));
    }

    /** Runs records from multiple source topics as one graph evaluation, enabling fan-in. */
    public synchronized List<ProducedToTopic> runBatches(Map<String, List<ConsumedRecord>> input) {
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
                    case OPERATOR -> frames.put(index, runOperator(index, node, frames));
                    case MERGE -> frames.put(index, merge(node, frames));
                    case SINK -> {
                        if (node.sinkCodec() == null) {
                            var parent = nodes.get(node.parents().get(0).index());
                            input.forEach((topic, records) -> {
                                if (parent.sourceTopics().contains(topic)) {
                                    records.forEach(record -> produced.add(new ProducedToTopic(
                                            node.sinkTopic(),
                                            new ProduceRecord(record.key(), record.value(), record.timestamp()))));
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
            int nodeIndex,
            ColumnarTopology.NodeDefinition node,
            Map<Integer, List<VectorSchemaRoot>> frames) {
        var outputs = new ArrayList<VectorSchemaRoot>();
        var processor = processors.get(nodeIndex);
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
}
