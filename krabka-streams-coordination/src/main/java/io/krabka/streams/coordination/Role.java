package io.krabka.streams.coordination;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * The validated name of one coordination role, such as the name of an active
 * controller.
 *
 * <p>A role becomes a Kafka {@code transactional.id}. The transaction coordinator mints
 * one producer epoch for that id, so the role names the fenced group of members that
 * compete for the same leadership. All records of one role go to one partition of
 * {@link CoordinationCodec#TOPIC}, and {@link RolePartitioner} computes that partition
 * from the name.
 *
 * <p>{@link #of(String)} is the only way to build a role, so every {@code Role} value
 * is proof that the name is well formed. The factory rejects an empty name and a name
 * longer than {@link #MAX_LENGTH} bytes.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * Role controller = Role.of("controller");
 * producerSettings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, controller.name());
 * }</pre>
 */
public final class Role implements Comparable<Role> {
    /**
     * The maximum length of a role name, in bytes.
     *
     * <p>The bound is the bound Kafka puts on a topic name, so a role stays short
     * enough to log and to compare.
     */
    public static final int MAX_LENGTH = 249;

    private static final String FIELD = "role";

    private final String name;

    private Role(String name) {
        this.name = name;
    }

    /**
     * Parses a role name.
     *
     * @param name the role name, in UTF-8
     * @return the validated role
     * @throws CoordinationException if the name is empty or holds more than
     *     {@link #MAX_LENGTH} bytes
     * @throws NullPointerException if the name is null
     */
    public static Role of(String name) {
        Names.check(name, FIELD, MAX_LENGTH);
        return new Role(name);
    }

    /**
     * Returns the role name.
     *
     * @return the name this role carries
     */
    public String name() {
        return name;
    }

    /**
     * Returns the UTF-8 bytes of the role name.
     *
     * <p>{@link RolePartitioner} hashes these bytes, and {@link CoordinationCodec}
     * writes them into a key.
     *
     * @return a fresh array of the name bytes
     */
    public byte[] bytes() {
        return name.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Orders two roles by their names.
     *
     * @param other the role to compare against
     * @return a negative number, zero, or a positive number as this name sorts before,
     *     with, or after the other name
     */
    @Override
    public int compareTo(Role other) {
        return name.compareTo(other.name);
    }

    /**
     * Reports whether another object is a role with the same name.
     *
     * @param other the object to compare against
     * @return true when the other object is a role of this name
     */
    @Override
    public boolean equals(Object other) {
        return other instanceof Role role && name.equals(role.name);
    }

    /**
     * Returns a hash of the role name.
     *
     * @return the hash code of the name
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    /**
     * Returns the role name.
     *
     * @return the name this role carries
     */
    @Override
    public String toString() {
        return name;
    }
}
