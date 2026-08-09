package io.krabka.streams.columnar;

import java.util.concurrent.atomic.LongAdder;

/** Lock-free counters for a columnar runner. */
public final class ColumnarMetrics {
    private final LongAdder batches = new LongAdder();
    private final LongAdder inputRecords = new LongAdder();
    private final LongAdder outputRecords = new LongAdder();
    private final LongAdder failures = new LongAdder();
    private final LongAdder deadLetterRecords = new LongAdder();
    private final LongAdder processingNanos = new LongAdder();

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

    public Snapshot snapshot() {
        return new Snapshot(
                batches.sum(),
                inputRecords.sum(),
                outputRecords.sum(),
                failures.sum(),
                deadLetterRecords.sum(),
                processingNanos.sum());
    }

    public record Snapshot(
            long batches,
            long inputRecords,
            long outputRecords,
            long failures,
            long deadLetterRecords,
            long processingNanos) {
    }
}
