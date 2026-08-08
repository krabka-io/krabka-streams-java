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

    BuiltColumnarTopology(ColumnarTopology topology) {
        this.topology = topology;
    }

    public List<ProducedToTopic> runBatch(String topic, List<ConsumedRecord> records) {
        Map<Integer, List<VectorSchemaRoot>> frames = new HashMap<>();
        var produced = new ArrayList<ProducedToTopic>();
        try {
            var nodes = topology.nodes();
            for (int index = 0; index < nodes.size(); index++) {
                var node = nodes.get(index);
                switch (node.type()) {
                    case SOURCE -> {
                        if (!records.isEmpty() && node.sourceTopics().contains(topic)) {
                            frames.put(index, List.of(node.sourceCodec().decode(records)));
                        } else {
                            frames.put(index, List.of());
                        }
                    }
                    case OPERATOR -> frames.put(index, runOperator(node, frames));
                    case SINK -> {
                        for (var batch : frames.getOrDefault(node.parent().index(), List.of())) {
                            for (var record : node.sinkCodec().encode(batch)) {
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
            ColumnarTopology.NodeDefinition node,
            Map<Integer, List<VectorSchemaRoot>> frames) {
        var outputs = new ArrayList<VectorSchemaRoot>();
        var processor = node.processor().get();
        for (var parent : frames.getOrDefault(node.parent().index(), List.of())) {
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
                input.close();
                context.drain().forEach(VectorSchemaRoot::close);
                outputs.forEach(VectorSchemaRoot::close);
                throw error;
            }
        }
        return List.copyOf(outputs);
    }
}
