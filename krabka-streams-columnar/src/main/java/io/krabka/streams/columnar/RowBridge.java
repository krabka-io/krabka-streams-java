package io.krabka.streams.columnar;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Converts typed rows to and from an Arrow payload batch.
 *
 * <p>A bridge defines how a Java value type maps onto Arrow columns; {@link RowCodec}
 * combines it with a Kafka serde to turn ordinary records into batches. Row order and
 * count must be preserved in both directions so metadata columns stay aligned with
 * their rows. {@link JsonRowBridge} is the bundled implementation for
 * JSON-compatible types.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * RowBridge<Transaction> bridge = new JsonRowBridge<>(Transaction.class);
 * try (var batch = bridge.rowsToBatch(transactions, allocator)) {
 *     List<Transaction> roundTripped = bridge.batchToRows(batch);
 * }
 * }</pre>
 *
 * @param <T> the row type the bridge converts
 */
public interface RowBridge<T> {
    /**
     * Builds one Arrow batch with one row per value.
     *
     * @param rows the values to convert, in record order
     * @param allocator the allocator that owns the batch's buffers
     * @return the payload batch; the caller must close it
     */
    VectorSchemaRoot rowsToBatch(List<T> rows, BufferAllocator allocator);

    /**
     * Converts an Arrow payload batch back into one value per row.
     *
     * @param batch the payload batch to convert; the bridge reads it and leaves it open
     * @return the values in row order
     */
    List<T> batchToRows(VectorSchemaRoot batch);
}
