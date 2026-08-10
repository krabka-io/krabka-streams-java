package io.krabka.streams.columnar;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Processes one Arrow batch and forwards zero or more output batches.
 *
 * <p>Register a processor with
 * {@link ColumnarTopology#addOperator(String, java.util.function.Supplier, ColumnarNode)}.
 * The supplier is invoked once per logical partition, so instance fields hold
 * partition-local state; implement {@link StatefulColumnarProcessor} when that state
 * must survive rebalances and restarts.
 *
 * <h2>Buffer ownership</h2>
 *
 * <ul>
 *   <li>The input batch belongs to the framework; never close it.
 *   <li>Forwarding the input unchanged is allowed; the framework keeps it alive.
 *   <li>A batch you create and forward transfers to the framework.
 *   <li>A batch you create and do not forward is yours to close.
 *   <li>If {@code process} throws, the framework closes the input and everything
 *       already forwarded, then rethrows.
 * </ul>
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * ColumnarProcessor dropEmpty = (context, batch) -> {
 *     if (batch.getRowCount() > 0) {
 *         context.forward(batch); // forwarding the input is allowed
 *     }
 *     // forwarding nothing drops the batch; the framework closes it
 * };
 * topology.addOperator("drop-empty", () -> dropEmpty, source);
 * }</pre>
 */
@FunctionalInterface
public interface ColumnarProcessor {
    /**
     * Processes one input batch.
     *
     * @param context the collector output batches are forwarded to
     * @param batch the input batch, owned by the framework
     */
    void process(ColumnarContext context, VectorSchemaRoot batch);
}
