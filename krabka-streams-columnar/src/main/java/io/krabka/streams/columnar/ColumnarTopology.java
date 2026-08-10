package io.krabka.streams.columnar;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.arrow.memory.BufferAllocator;

/**
 * A linear or branching graph whose edges carry Arrow batches.
 *
 * <p>Nodes are added in evaluation order: every {@code add*} method returns a
 * {@link ColumnarNode} handle that downstream nodes name as their parent, so a parent
 * always precedes its children. A topology needs at least one source and one sink;
 * {@link #build()} validates the graph and returns a reusable
 * {@link BuiltColumnarTopology} that holds the per-partition processor state.
 *
 * <p>The topology itself is cheap and stateless; all processing state lives in the
 * built form. Build once and keep the built topology for the lifetime of a consumer
 * group member so stateful operators accumulate across polls.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try (var allocator = new RootAllocator()) {
 *     var codec = new BlobCodec(allocator);
 *     var topology = new ColumnarTopology(allocator);
 *
 *     var source = topology.addSource("source", List.of("transactions"), codec);
 *     var large = topology.addOperator(
 *         "large",
 *         BuiltinOp.filter(allocator, (batch, row) ->
 *             ((BigIntVector) batch.getVector("amount")).get(row) > 4),
 *         source);
 *     topology.addSink("sink", "large-transactions", codec, large);
 *
 *     try (var built = topology.build()) {
 *         var output = built.runBatch("transactions", records);
 *     }
 * }
 * }</pre>
 */
public final class ColumnarTopology {
    private final BufferAllocator allocator;
    private final List<NodeDefinition> nodes = new ArrayList<>();

    /**
     * Creates an empty topology.
     *
     * @param allocator the allocator that owns every batch the topology creates; use
     *     one allocator per application and close it at shutdown
     */
    public ColumnarTopology(BufferAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
    }

    /**
     * Adds a source that decodes records from one or more topics.
     *
     * <p>All topics of one source must share the codec and, per graph evaluation,
     * their decoded batches flow separately into the source's children.
     *
     * @param name the unique node name
     * @param topics the topics the source consumes; at least one
     * @param codec the codec that decodes fetched records
     * @return the handle downstream nodes use as their parent
     * @throws IllegalArgumentException if {@code topics} is empty
     */
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
                null,
                List.of()));
    }

    /**
     * Adds a built-in operator node.
     *
     * <p>The operator is copied per logical partition, so the same {@link BuiltinOp}
     * instance can be reused in several topologies and its state stays
     * partition-local.
     *
     * @param name the unique node name; stateful operators are snapshotted under it
     * @param operator the built-in operator to apply
     * @param parent the upstream node whose batches the operator processes
     * @return the handle downstream nodes use as their parent
     * @throws IllegalArgumentException if {@code parent} is not a node of this topology
     */
    public ColumnarNode addOperator(String name, BuiltinOp operator, ColumnarNode parent) {
        return addOperator(name, Objects.requireNonNull(operator, "operator")::fresh, parent);
    }

    /**
     * Adds a custom operator node.
     *
     * <p>The supplier is invoked once per logical partition, which is what makes
     * instance fields of the processor partition-local state. Implement
     * {@link StatefulColumnarProcessor} to participate in snapshot and restore.
     *
     * @param name the unique node name; stateful operators are snapshotted under it
     * @param processor the factory invoked once per logical partition
     * @param parent the upstream node whose batches the processor processes
     * @return the handle downstream nodes use as their parent
     * @throws IllegalArgumentException if {@code parent} is not a node of this topology
     */
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
                null,
                List.of(parent)));
    }

    /**
     * Merges batches from two or more upstream branches with the same Arrow schema.
     *
     * <p>Per evaluation, the parents' batches are concatenated into one batch in
     * parent order. Parents whose schemas differ make the evaluation throw
     * {@link ColumnarException}.
     *
     * @param name the unique node name
     * @param parents the branches to merge; at least two
     * @return the handle downstream nodes use as their parent
     * @throws IllegalArgumentException if fewer than two parents are given or one of
     *     them is not a node of this topology
     */
    public ColumnarNode addMerge(String name, Collection<ColumnarNode> parents) {
        var copied = List.copyOf(parents);
        if (copied.size() < 2) {
            throw new IllegalArgumentException("a merge needs at least two parents");
        }
        copied.forEach(this::requireParent);
        return add(new NodeDefinition(name, NodeType.MERGE, List.of(), null, null, null, null, null, copied));
    }

    /**
     * Joins two co-partitioned branches by key within an event-time window.
     *
     * <p>The join is stateful: rows are retained per logical partition until they age
     * past the window, so late arrivals on either side still match. Join state
     * participates in snapshot and restore under this node's name.
     *
     * @param name the unique node name; join state is snapshotted under it
     * @param join the key columns, window, and output prefixes
     * @param left the left branch
     * @param right the right branch
     * @return the handle downstream nodes use as their parent
     * @throws IllegalArgumentException if the parents are the same node or either is
     *     not a node of this topology
     */
    public ColumnarNode addJoin(
            String name, ColumnarJoin join, ColumnarNode left, ColumnarNode right) {
        requireParent(left);
        requireParent(right);
        if (left.equals(right)) {
            throw new IllegalArgumentException("a join needs two different parents");
        }
        return add(new NodeDefinition(
                name,
                NodeType.JOIN,
                List.of(),
                null,
                null,
                null,
                null,
                Objects.requireNonNull(join, "join"),
                List.of(left, right)));
    }

    /**
     * Adds a sink that encodes its parent's batches to a topic.
     *
     * @param name the unique node name
     * @param topic the topic produced records are destined for
     * @param codec the codec that encodes batches into records
     * @param parent the upstream node whose batches the sink encodes
     * @return the sink's handle
     * @throws IllegalArgumentException if {@code parent} is not a node of this topology
     */
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
                null,
                List.of(parent)));
    }

    /**
     * Copies source records byte-for-byte to a topic without decoding and re-encoding
     * the sink.
     *
     * <p>Use this for mirroring or teeing a source topic alongside processing: the
     * source's records keep their exact key, value, timestamp, and headers. When the
     * source feeds only pass-through sinks, its records are never decoded at all.
     *
     * @param name the unique node name
     * @param topic the topic the copies are destined for
     * @param source the source node whose records are copied
     * @return the sink's handle
     * @throws IllegalArgumentException if {@code source} is not a source node of this
     *     topology
     */
    public ColumnarNode addPassThroughSink(String name, String topic, ColumnarNode source) {
        requireParent(source);
        if (nodes.get(source.index()).type() != NodeType.SOURCE) {
            throw new IllegalArgumentException("a pass-through sink must be attached directly to a source");
        }
        return add(new NodeDefinition(
                name,
                NodeType.SINK,
                List.of(),
                null,
                null,
                Objects.requireNonNull(topic, "topic"),
                null,
                null,
                List.of(source)));
    }

    /**
     * Returns every topic consumed by the topology's sources.
     *
     * @return the source topics in node insertion order
     */
    public List<String> sourceTopics() {
        return nodes.stream()
                .filter(node -> node.type() == NodeType.SOURCE)
                .flatMap(node -> node.sourceTopics().stream())
                .toList();
    }

    /**
     * Checks the graph without building it.
     *
     * <p>{@link #build()} calls this automatically; call it directly to fail fast in
     * tests or while assembling a topology dynamically.
     *
     * @throws ColumnarException if a node name repeats, a non-source node lacks a
     *     valid earlier parent, or the topology has no source or no sink
     */
    public void validate() {
        var names = new HashSet<String>();
        int sourceCount = 0;
        int sinkCount = 0;
        for (int index = 0; index < nodes.size(); index++) {
            var node = nodes.get(index);
            int nodeIndex = index;
            if (!names.add(node.name())) {
                throw new ColumnarException("duplicate node name `" + node.name() + "`");
            }
            if (node.type() == NodeType.SOURCE) {
                sourceCount++;
            } else if (node.parents().isEmpty()
                    || node.parents().stream().anyMatch(parent -> parent.index() >= nodeIndex)) {
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

    /**
     * Validates the graph and creates its executable form.
     *
     * <p>Each build creates fresh processor state; nodes added to this topology after
     * building are not part of the built instance.
     *
     * @return the built topology; close it to release processor state
     * @throws ColumnarException if validation fails
     */
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
        MERGE,
        JOIN,
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
            ColumnarJoin join,
            List<ColumnarNode> parents) {
    }
}
