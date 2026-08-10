package io.krabka.streams.columnar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Executes a {@link ColumnarJoin}: retains both sides' rows per logical partition as
 * serialized Arrow batches, matches keys within the event-time window, prunes rows
 * that age past it, and snapshots its retained batches for restore.
 */
final class StatefulJoinProcessor implements StatefulColumnarProcessor {
    private static final int SNAPSHOT_VERSION = 1;
    private final ColumnarJoin join;
    private final BufferAllocator allocator;
    private final ArrowIpcSerde serde;
    private final List<TimedBatch> left = new ArrayList<>();
    private final List<TimedBatch> right = new ArrayList<>();
    private long streamTime = Long.MIN_VALUE;

    StatefulJoinProcessor(ColumnarJoin join, BufferAllocator allocator) {
        this.join = Objects.requireNonNull(join, "join");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        serde = new ArrowIpcSerde(allocator);
    }

    List<VectorSchemaRoot> process(List<VectorSchemaRoot> newLeft, List<VectorSchemaRoot> newRight) {
        newLeft.forEach(this::advanceStreamTime);
        newRight.forEach(this::advanceStreamTime);
        prune(left);
        prune(right);
        var outputs = new ArrayList<VectorSchemaRoot>();
        try {
            for (var batch : newLeft) {
                joinStored(batch, right, true, outputs);
                for (var other : newRight) {
                    addMatches(batch, other, outputs);
                }
            }
            for (var batch : newRight) {
                joinStored(batch, left, false, outputs);
            }
            newLeft.forEach(batch -> left.add(stored(batch)));
            newRight.forEach(batch -> right.add(stored(batch)));
            return List.copyOf(outputs);
        } catch (RuntimeException error) {
            outputs.forEach(VectorSchemaRoot::close);
            throw error;
        }
    }

    @Override
    public void process(ColumnarContext context, VectorSchemaRoot batch) {
        throw new UnsupportedOperationException("a join requires two parents");
    }

    @Override
    public byte[] snapshot() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var output = new DataOutputStream(bytes)) {
                output.writeInt(SNAPSHOT_VERSION);
                output.writeLong(streamTime);
                writeBatches(output, left);
                writeBatches(output, right);
            }
            return bytes.toByteArray();
        } catch (IOException error) {
            throw new AssertionError(error);
        }
    }

    @Override
    public void restore(byte[] snapshot) {
        try (var input = new DataInputStream(new ByteArrayInputStream(snapshot))) {
            if (input.readInt() != SNAPSHOT_VERSION) {
                throw new ColumnarException("unsupported join snapshot version");
            }
            streamTime = input.readLong();
            left.clear();
            left.addAll(readBatches(input));
            right.clear();
            right.addAll(readBatches(input));
            if (input.read() != -1) {
                throw new ColumnarException("trailing bytes in join snapshot");
            }
        } catch (IOException error) {
            throw new ColumnarException("cannot restore join state", error);
        }
    }

    private void joinStored(
            VectorSchemaRoot batch,
            List<TimedBatch> stored,
            boolean batchIsLeft,
            List<VectorSchemaRoot> outputs) {
        for (var value : stored) {
            try (var decoded = serde.deserialize(value.bytes())) {
                if (batchIsLeft) {
                    addMatches(batch, decoded, outputs);
                } else {
                    addMatches(decoded, batch, outputs);
                }
            }
        }
    }

    private void addMatches(
            VectorSchemaRoot leftBatch,
            VectorSchemaRoot rightBatch,
            List<VectorSchemaRoot> outputs) {
        var leftKeys = required(leftBatch, join.leftKey());
        var rightKeys = required(rightBatch, join.rightKey());
        var leftTimestamps = timestamps(leftBatch);
        var rightTimestamps = timestamps(rightBatch);
        var pairs = new ArrayList<ArrowBatchSupport.RowPair>();
        // ponytail: nested scan is bounded by the join window; add a hash index if profiling demands it.
        for (int leftRow = 0; leftRow < leftBatch.getRowCount(); leftRow++) {
            var leftKey = ArrowBatchSupport.value(leftKeys, leftRow);
            if (leftKey == null) {
                continue;
            }
            for (int rightRow = 0; rightRow < rightBatch.getRowCount(); rightRow++) {
                if (Objects.equals(leftKey, ArrowBatchSupport.value(rightKeys, rightRow))
                        && withinWindow(leftTimestamps.get(leftRow), rightTimestamps.get(rightRow))) {
                    pairs.add(new ArrowBatchSupport.RowPair(leftRow, rightRow));
                }
            }
        }
        if (!pairs.isEmpty()) {
            outputs.add(ArrowBatchSupport.joinRows(
                    leftBatch,
                    rightBatch,
                    pairs,
                    join.leftPrefix(),
                    join.rightPrefix(),
                    allocator));
        }
    }

    private boolean withinWindow(long leftTimestamp, long rightTimestamp) {
        try {
            long difference = Math.subtractExact(leftTimestamp, rightTimestamp);
            return difference != Long.MIN_VALUE && Math.abs(difference) <= join.window().toMillis();
        } catch (ArithmeticException ignored) {
            return false;
        }
    }

    private void advanceStreamTime(VectorSchemaRoot batch) {
        var timestamps = timestamps(batch);
        for (int row = 0; row < batch.getRowCount(); row++) {
            if (!timestamps.isNull(row)) {
                streamTime = Math.max(streamTime, timestamps.get(row));
            }
        }
    }

    private void prune(List<TimedBatch> batches) {
        long computedCutoff;
        try {
            computedCutoff = Math.subtractExact(streamTime, join.window().toMillis());
        } catch (ArithmeticException ignored) {
            computedCutoff = Long.MIN_VALUE;
        }
        final long cutoff = computedCutoff;
        batches.removeIf(batch -> batch.maxTimestamp() < cutoff);
    }

    private TimedBatch stored(VectorSchemaRoot batch) {
        long maximum = Long.MIN_VALUE;
        var timestamps = timestamps(batch);
        for (int row = 0; row < batch.getRowCount(); row++) {
            if (!timestamps.isNull(row)) {
                maximum = Math.max(maximum, timestamps.get(row));
            }
        }
        return new TimedBatch(maximum, serde.serialize(batch));
    }

    private static org.apache.arrow.vector.FieldVector required(VectorSchemaRoot batch, String name) {
        var vector = batch.getVector(name);
        if (vector == null) {
            throw new ColumnarException("join key column does not exist: " + name);
        }
        return vector;
    }

    private static BigIntVector timestamps(VectorSchemaRoot batch) {
        var vector = batch.getVector(ArrowBatchSupport.TIMESTAMP);
        if (vector instanceof BigIntVector timestamps) {
            return timestamps;
        }
        throw new ColumnarException("join requires " + ArrowBatchSupport.TIMESTAMP);
    }

    private static void writeBatches(DataOutputStream output, List<TimedBatch> batches) throws IOException {
        output.writeInt(batches.size());
        for (var batch : batches) {
            var bytes = batch.bytes();
            output.writeLong(batch.maxTimestamp());
            output.writeInt(bytes.length);
            output.write(bytes);
        }
    }

    private static List<TimedBatch> readBatches(DataInputStream input) throws IOException {
        int count = input.readInt();
        if (count < 0) {
            throw new ColumnarException("negative join snapshot batch count");
        }
        var batches = new ArrayList<TimedBatch>(count);
        for (int index = 0; index < count; index++) {
            long maxTimestamp = input.readLong();
            int length = input.readInt();
            if (length < 0) {
                throw new ColumnarException("negative join snapshot batch length");
            }
            var bytes = input.readNBytes(length);
            if (bytes.length != length) {
                throw new ColumnarException("truncated join snapshot");
            }
            batches.add(new TimedBatch(maxTimestamp, bytes));
        }
        return batches;
    }

    private record TimedBatch(long maxTimestamp, byte[] bytes) {
        private TimedBatch {
            bytes = bytes.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }
    }
}
