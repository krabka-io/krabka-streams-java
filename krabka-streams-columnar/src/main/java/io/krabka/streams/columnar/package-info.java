/**
 * Apache Arrow batch codecs, topology operators, runners, and state handling.
 *
 * <p>This package processes Kafka records as Arrow batches instead of one record at a
 * time. A {@link io.krabka.streams.columnar.BatchCodec} decodes a fetched partition
 * batch into one {@code VectorSchemaRoot}, a
 * {@link io.krabka.streams.columnar.ColumnarTopology} routes whole batches through
 * operators, and a {@link io.krabka.streams.columnar.ColumnarRunner} drives fetch,
 * process, produce, and commit cycles against plain Kafka clients. Stateful operators
 * keep partition-local state that can be snapshotted into a
 * {@link io.krabka.streams.columnar.ColumnarStateStore}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try (var allocator = new RootAllocator()) {
 *   var codec = new BlobCodec(allocator);
 *   var topology = new ColumnarTopology(allocator);
 *   var source = topology.addSource("source", List.of("transactions"), codec);
 *   topology.addSink("sink", "processed-transactions", codec, source);
 *   try (var built = topology.build()) {
 *     var output = built.runBatch("transactions", records);
 *   }
 * }
 * }</pre>
 */
package io.krabka.streams.columnar;
