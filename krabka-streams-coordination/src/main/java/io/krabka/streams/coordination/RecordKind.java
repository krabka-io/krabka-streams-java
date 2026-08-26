package io.krabka.streams.coordination;

/**
 * Which record of {@link CoordinationCodec#TOPIC} a key names.
 *
 * <p>The key carries the kind, so one topic holds both kinds and one compaction key
 * covers both. The two kinds differ in what they claim. A registration announces that a
 * member is available, and it carries no authority. A lease names the member that holds
 * a role now, and its author held the epoch of the role when it wrote the record.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * CoordinationKey key = CoordinationCodec.decodeKey(record.key());
 * if (key.kind() == RecordKind.LEASE) {
 *     System.out.println(key.role() + " changed hands");
 * }
 * }</pre>
 */
public enum RecordKind {
    /** One member of a role announces that it is available. */
    REGISTRATION((short) 0),

    /** One role names the member that holds it now. */
    LEASE((short) 1);

    private final short code;

    RecordKind(short code) {
        this.code = code;
    }

    /**
     * Returns the wire code a writer puts in the key.
     *
     * @return {@code 0} for {@link #REGISTRATION} and {@code 1} for {@link #LEASE}
     */
    public short code() {
        return code;
    }

    /**
     * Maps a wire code onto a record kind.
     *
     * @param code the kind field read from a key
     * @return the matching record kind
     * @throws CoordinationException if no record kind carries the code
     */
    public static RecordKind fromCode(short code) {
        for (RecordKind kind : values()) {
            if (kind.code == code) {
                return kind;
            }
        }
        throw new CoordinationException(
                "malformed coordination state key: unknown record kind " + code);
    }
}
