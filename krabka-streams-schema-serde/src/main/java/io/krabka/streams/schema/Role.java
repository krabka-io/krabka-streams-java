package io.krabka.streams.schema;

/**
 * Identifies whether a schema belongs to a record key or value.
 *
 * <p>The role selects the registry subject through the configured
 * {@link SubjectNameStrategy} (for example {@code orders-key} versus
 * {@code orders-value} under the default {@link TopicNameStrategy}) and is checked
 * against Kafka's {@code isKey} flag when a serde is configured.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var keySerde = AvroSerde.forKey(OrderKey.class, cache);     // Role.KEY
 * var valueSerde = AvroSerde.forValue(Order.class, cache);    // Role.VALUE
 * var generic = AvroSerde.generic(schema, cache, Role.VALUE); // explicit role
 * }</pre>
 */
public enum Role {
    /** The schema describes the record key. */
    KEY,

    /** The schema describes the record value. */
    VALUE
}
