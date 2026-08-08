package io.krabka.streams.columnar;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.arrow.memory.BufferAllocator;

/** A linear or branching graph whose edges carry Arrow batches. */
public final class ColumnarTopology {
    private final BufferAllocator allocator;
    private final List<NodeDefinition> nodes = new ArrayList<>();

    public ColumnarTopology(BufferAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
    }

    public ColumnarNode addSource(String name, Collection<String> topics, BatchCodec codec) {
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("a source needs at least one topic");
        }
        return add(new NodeDefinition(
                name,
                NodeType.SOURCE,
                List.copyOf(topics),
                Objects.requireNonNull(codec, "codec"),
                null,
                null,
                null,
                null));
    }

    public ColumnarNode addOperator(String name, BuiltinOp operator, ColumnarNode parent) {
        return addOperator(name, () -> operator, parent);
    }

    public ColumnarNode addOperator(
            String name, Supplier<? extends ColumnarProcessor> processor, ColumnarNode parent) {
        requireParent(parent);
        return add(new NodeDefinition(
                name,
                NodeType.OPERATOR,
                List.of(),
                null,
                Objects.requireNonNull(processor, "processor"),
                null,
                null,
                parent));
    }

    public ColumnarNode addSink(String name, String topic, BatchCodec codec, ColumnarNode parent) {
        requireParent(parent);
        return add(new NodeDefinition(
                name,
                NodeType.SINK,
                List.of(),
                null,
                null,
                Objects.requireNonNull(topic, "topic"),
                Objects.requireNonNull(codec, "codec"),
                parent));
    }

    public List<String> sourceTopics() {
        return nodes.stream()
                .filter(node -> node.type() == NodeType.SOURCE)
                .flatMap(node -> node.sourceTopics().stream())
                .toList();
    }

    public void validate() {
        var names = new HashSet<String>();
        int sourceCount = 0;
        int sinkCount = 0;
        for (int index = 0; index < nodes.size(); index++) {
            var node = nodes.get(index);
            if (!names.add(node.name())) {
                throw new ColumnarException("duplicate node name `" + node.name() + "`");
            }
            if (node.type() == NodeType.SOURCE) {
                sourceCount++;
            } else if (node.parent() == null || node.parent().index() >= index) {
                throw new ColumnarException("node `" + node.name() + "` has an invalid parent");
            }
            if (node.type() == NodeType.SINK) {
                sinkCount++;
            }
        }
        if (sourceCount == 0) {
            throw new ColumnarException("topology has no source");
        }
        if (sinkCount == 0) {
            throw new ColumnarException("topology has no sink");
        }
    }

    public BuiltColumnarTopology build() {
        validate();
        return new BuiltColumnarTopology(this);
    }

    BufferAllocator allocator() {
        return allocator;
    }

    List<NodeDefinition> nodes() {
        return List.copyOf(nodes);
    }

    private ColumnarNode add(NodeDefinition definition) {
        Objects.requireNonNull(definition.name(), "name");
        var node = new ColumnarNode(this, nodes.size());
        nodes.add(definition);
        return node;
    }

    private void requireParent(ColumnarNode parent) {
        if (parent == null
                || !parent.belongsTo(this)
                || parent.index() < 0
                || parent.index() >= nodes.size()) {
            throw new IllegalArgumentException("parent is not a node in this topology");
        }
    }

    enum NodeType {
        SOURCE,
        OPERATOR,
        SINK
    }

    record NodeDefinition(
            String name,
            NodeType type,
            List<String> sourceTopics,
            BatchCodec sourceCodec,
            Supplier<? extends ColumnarProcessor> processor,
            String sinkTopic,
            BatchCodec sinkCodec,
            ColumnarNode parent) {
    }
}
