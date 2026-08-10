package io.krabka.streams.columnar;

import java.util.Objects;

/**
 * Binds one produced record to its sink topic.
 *
 * <p>A topology evaluation returns these so a single result list can carry the output
 * of several sinks. {@link ColumnarRunner#sendAsync(java.util.List, org.apache.kafka.clients.producer.Producer)}
 * sends each record to its topic.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * List<ProducedToTopic> outputs = built.runBatch("transactions", records);
 * for (var output : outputs) {
 *     producerLog.info("{} -> {} bytes", output.topic(), output.record().value().length);
 * }
 * }</pre>
 *
 * @param topic the sink topic the record is destined for
 * @param record the record to produce
 */
public record ProducedToTopic(String topic, ProduceRecord record) {
    /**
     * Validates that both components are present.
     *
     * @param topic the sink topic the record is destined for
     * @param record the record to produce
     * @throws NullPointerException if {@code topic} or {@code record} is null
     */
    public ProducedToTopic {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(record, "record");
    }
}
