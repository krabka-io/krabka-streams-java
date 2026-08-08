package io.krabka.streams.columnar;

import java.util.Objects;

/** Binds one produced record to its sink topic. */
public record ProducedToTopic(String topic, ProduceRecord record) {
    public ProducedToTopic {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(record, "record");
    }
}
