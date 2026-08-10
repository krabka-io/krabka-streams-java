package io.krabka.streams.columnar;

import java.util.concurrent.atomic.LongAdder;

/**
 * Lock-free counters for a columnar runner.
 *
 * <p>Group runners record one observation per logical partition batch. Counters only
 * grow; readers take consistent-enough views with {@link #snapshot()} and compute
 * rates by differencing two snapshots. One instance can be shared by several runners
 * to aggregate their throughput.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var metrics = new ColumnarMetrics();
 * var runner = ColumnarRunner.group(
 *     topology, consumer, producer, ColumnarErrorPolicy.fail(),
 *     ColumnarStateStore.none(), metrics);
 *
 * runner.runOnce(Duration.ofSeconds(1));
 * var snapshot = metrics.snapshot();
 * log.info("{} records in {} batches", snapshot.inputRecords(), snapshot.batches());
 * }</pre>
 */
public final class ColumnarMetrics {
    private final LongAdder batches = new LongAdder();
    private final LongAdder inputRecords = new LongAdder();
    private final LongAdder outputRecords = new LongAdder();
    private final LongAdder failures = new LongAdder();
    private final LongAdder deadLetterRecords = new LongAdder();
    private final LongAdder processingNanos = new LongAdder();

    /** Creates a metrics object with all counters at zero. */
    public ColumnarMetrics() {
    }

    void recordBatch(int input, int output, long nanos) {
        batches.increment();
        inputRecords.add(input);
        outputRecords.add(output);
        processingNanos.add(nanos);
    }

    void recordFailure(int input, int deadLetters, long nanos) {
        batches.increment();
        inputRecords.add(input);
        failures.increment();
        deadLetterRecords.add(deadLetters);
        processingNanos.add(nanos);
    }

    /**
     * Reads all counters into one immutable snapshot.
     *
     * <p>Counters are read individually without a global lock, so a snapshot taken
     * while runners are active may straddle an in-flight batch; totals are still
     * monotonic between snapshots.
     *
     * @return the current counter values
     */
    public Snapshot snapshot() {
        return new Snapshot(
                batches.sum(),
                inputRecords.sum(),
                outputRecords.sum(),
                failures.sum(),
                deadLetterRecords.sum(),
                processingNanos.sum());
    }

    /**
     * One point-in-time view of the counters.
     *
     * @param batches how many logical partition batches were processed, including failed ones
     * @param inputRecords how many records entered processing
     * @param outputRecords how many records the topology produced
     * @param failures how many partition batches failed
     * @param deadLetterRecords how many records were forwarded to a dead-letter topic
     * @param processingNanos total processing time in nanoseconds, excluding produce
     *     and commit time
     */
    public record Snapshot(
            long batches,
            long inputRecords,
            long outputRecords,
            long failures,
            long deadLetterRecords,
            long processingNanos) {
    }
}
