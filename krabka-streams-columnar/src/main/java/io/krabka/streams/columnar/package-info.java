/**
 * Apache Arrow batch codecs, topology operators, runners, and state handling.
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
