package io.krabka.streams.schema;

/**
 * Maps a topic and record role to a schema registry subject.
 *
 * <p>The default strategy is {@link TopicNameStrategy}. Provide a custom
 * implementation to serdes or to {@link SchemaCache} when subjects follow a different
 * convention, such as one subject per record type shared across topics.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * SubjectNameStrategy recordName = (topic, role) -> "com.example.Order";
 * var serde = AvroSerde.forValue(Order.class, cache, recordName);
 * }</pre>
 */
@FunctionalInterface
public interface SubjectNameStrategy {
    /**
     * Returns the registry subject for a topic and role.
     *
     * @param topic the Kafka topic name
     * @param role whether the schema describes the record key or value
     * @return the registry subject name
     */
    String subject(String topic, Role role);
}
