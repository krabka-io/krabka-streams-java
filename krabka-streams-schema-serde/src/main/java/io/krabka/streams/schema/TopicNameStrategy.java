package io.krabka.streams.schema;

import java.util.Objects;

/**
 * Uses the Confluent topic naming rule.
 *
 * <p>A key schema maps to {@code <topic>-key} and a value schema to
 * {@code <topic>-value}. This matches Confluent's default {@code TopicNameStrategy},
 * so applications sharing topics with Confluent-configured clients resolve the same
 * subjects.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var strategy = new TopicNameStrategy();
 * strategy.subject("orders", Role.KEY);   // "orders-key"
 * strategy.subject("orders", Role.VALUE); // "orders-value"
 * }</pre>
 */
public final class TopicNameStrategy implements SubjectNameStrategy {
    /** Creates the strategy; it is stateless and reusable. */
    public TopicNameStrategy() {
    }

    /**
     * Returns {@code <topic>-key} or {@code <topic>-value} depending on the role.
     *
     * @param topic the Kafka topic name
     * @param role whether the schema describes the record key or value
     * @return the topic name with a {@code -key} or {@code -value} suffix
     * @throws NullPointerException if {@code topic} or {@code role} is null
     */
    @Override
    public String subject(String topic, Role role) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(role, "role");
        return topic + (role == Role.KEY ? "-key" : "-value");
    }
}
