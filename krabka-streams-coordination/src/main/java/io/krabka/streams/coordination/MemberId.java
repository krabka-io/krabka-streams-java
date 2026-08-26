package io.krabka.streams.coordination;

import java.util.Objects;

/**
 * The validated identity of one member that competes for a role.
 *
 * <p>A member picks its own id and keeps that id across a reconnect, so the succession
 * order survives a short network failure. The id is the compaction key of the
 * registration record of the member, so two members of one role need two ids.
 *
 * <p>{@link #of(String)} is the only way to build a member id, so every
 * {@code MemberId} value is proof that the id is well formed. The factory rejects an
 * empty id and an id longer than {@link #MAX_LENGTH} bytes.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * MemberId me = MemberId.of(InetAddress.getLocalHost().getHostName());
 * transport.register(Role.of("controller"), me);
 * }</pre>
 */
public final class MemberId implements Comparable<MemberId> {
    /** The maximum length of a member id, in bytes. */
    public static final int MAX_LENGTH = 249;

    private static final String FIELD = "member id";

    private final String id;

    private MemberId(String id) {
        this.id = id;
    }

    /**
     * Parses a member id.
     *
     * @param id the member id, in UTF-8
     * @return the validated member id
     * @throws CoordinationException if the id is empty or holds more than
     *     {@link #MAX_LENGTH} bytes
     * @throws NullPointerException if the id is null
     */
    public static MemberId of(String id) {
        Names.check(id, FIELD, MAX_LENGTH);
        return new MemberId(id);
    }

    /**
     * Returns the member id.
     *
     * @return the id this member carries
     */
    public String id() {
        return id;
    }

    /**
     * Orders two member ids by their text.
     *
     * @param other the member id to compare against
     * @return a negative number, zero, or a positive number as this id sorts before,
     *     with, or after the other id
     */
    @Override
    public int compareTo(MemberId other) {
        return id.compareTo(other.id);
    }

    /**
     * Reports whether another object is a member id with the same text.
     *
     * @param other the object to compare against
     * @return true when the other object is a member id of this text
     */
    @Override
    public boolean equals(Object other) {
        return other instanceof MemberId member && id.equals(member.id);
    }

    /**
     * Returns a hash of the member id.
     *
     * @return the hash code of the id
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    /**
     * Returns the member id.
     *
     * @return the id this member carries
     */
    @Override
    public String toString() {
        return id;
    }
}
