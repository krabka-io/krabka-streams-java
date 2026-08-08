package io.krabka.streams.schema;

/** Maps a topic and record role to a schema registry subject. */
@FunctionalInterface
public interface SubjectNameStrategy {
    String subject(String topic, Role role);
}
