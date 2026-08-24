/**
 * Barrier-aligned, epoch-keyed state snapshots for columnar runners.
 *
 * <p>A krabka broker injects epoch-stamped markers into every partition of a named
 * barrier group and publishes the resulting cut to the internal
 * {@code __barrier_state} topic. A cut is an exact, reproducible point in every input
 * at once. The markers are Kafka control records, so a JVM consumer never delivers
 * one; this package reads the published cut instead and compares consumed offsets
 * against it.
 *
 * <p>{@link io.krabka.streams.columnar.barrier.BarrierCutDecoder} decodes the topic's
 * records, {@link io.krabka.streams.columnar.barrier.BarrierCutReader} reads a group's
 * complete cuts, and
 * {@link io.krabka.streams.columnar.barrier.BarrierAlignment} tells a group runner to
 * align on them. The runner snapshots each partition at the cut, keys the snapshot by
 * epoch, and can restore back to it later.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var alignment = BarrierAlignment.on("transactions", new BarrierCutReader(cutConsumer));
 * try (var runner = ColumnarRunner.group(topology, consumer, producer,
 *         ColumnarErrorPolicy.fail(), new FileColumnarStateStore(directory),
 *         new ColumnarMetrics(), alignment)) {
 *   runner.runOnce(Duration.ofSeconds(1));
 *   runner.restoreToLatestCut();
 * }
 * }</pre>
 */
package io.krabka.streams.columnar.barrier;
