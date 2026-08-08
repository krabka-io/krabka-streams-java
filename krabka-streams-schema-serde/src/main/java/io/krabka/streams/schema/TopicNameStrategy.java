package io.krabka.streams.schema;

import java.util.Objects;

/** Uses the Confluent topic naming rule. */
public final class TopicNameStrategy implements SubjectNameStrategy {
    @Override
    public String subject(String topic, Role role) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(role, "role");
        return topic + (role == Role.KEY ? "-key" : "-value");
    }
}
