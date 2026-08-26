package io.krabka.streams.coordination;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

/**
 * Encodes and decodes the frozen records of the internal {@code __coordination_state}
 * topic.
 *
 * <p>A member of a coordination role writes its registration and its lease to this
 * compacted topic. The key carries the record kind, so one topic holds both kinds. The
 * key is also the compaction key, so the topic keeps the last record of every role and
 * every member. A record with a null value is a tombstone. A tombstone of a
 * registration key deregisters one member, and a tombstone of a lease key clears the
 * lease of one role.
 *
 * <p>The layouts below are frozen. {@code krabka-client-rs} and
 * {@code krabka-streams-go} implement them byte for byte, and all three projects assert
 * the same golden bytes. Do not change a field, a field order, or a version number
 * without the same change in the two ports.
 *
 * <pre>
 * key:
 *   version  i16 = 0
 *   kind     i16          0 registration, 1 lease
 *   role     string
 *   member   string       the member id for kind 0, the empty string for kind 1
 *
 * registration value:
 *   version        i16 = 0
 *   member         string
 *   registered_at  i64    epoch milliseconds
 *
 * lease value:
 *   version         i16 = 0
 *   member          string
 *   producer_id     i64
 *   producer_epoch  i16
 *   granted_at      i64   epoch milliseconds
 *   deadline        i64   epoch milliseconds
 * </pre>
 *
 * <p>Every integer is big-endian and signed. A string is an {@code i16} byte length and
 * then plain UTF-8 bytes. This is Kafka's own native string layout, and it is the
 * layout that {@code BarrierCutDecoder} documents for {@code __barrier_state}. It is
 * not Java's modified UTF-8, which {@code DataOutput.writeUTF} produces. The length is
 * never negative, because the format has no null string. An absent member is the empty
 * string, and it encodes as the two bytes {@code 00 00}.
 *
 * <p>Malformed bytes raise {@link CoordinationException}. The decoder never returns a
 * partly filled record, and it never sizes an allocation from a length it read.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * byte[] key = CoordinationCodec.encodeKey(CoordinationKey.lease(role));
 * byte[] value = CoordinationCodec.encodeValue(lease);
 * producer.send(new ProducerRecord<>(CoordinationCodec.TOPIC, partition, key, value));
 *
 * CoordinationKey decoded = CoordinationCodec.decodeKey(key);
 * Optional<CoordinationValue> back = CoordinationCodec.decodeValue(decoded.kind(), value);
 * }</pre>
 */
public final class CoordinationCodec {
    /**
     * The compacted internal topic that holds the coordination state of a cluster.
     *
     * <p>Every record of one role goes to one partition, so the records of a role carry
     * a total order. See {@link RolePartitioner}. The topic needs
     * {@code cleanup.policy=compact}. A client writes it with {@code acks=all}, and an
     * operator should set {@code min.insync.replicas} to at least 2.
     */
    public static final String TOPIC = "__coordination_state";

    /** The version that every key and every value of the topic carries. */
    private static final short RECORD_VERSION = 0;

    private static final String KEY_PART = "key";
    private static final String REGISTRATION_VALUE_PART = "registration value";
    private static final String LEASE_VALUE_PART = "lease value";

    private CoordinationCodec() {
    }

    /**
     * Encodes a record key.
     *
     * <p>The method writes the empty member string for a lease key, because the frozen
     * layout gives a lease no member.
     *
     * @param key the key to encode
     * @return the key bytes of the frozen layout
     */
    public static byte[] encodeKey(CoordinationKey key) {
        Objects.requireNonNull(key, "key");
        byte[] role = key.role().bytes();
        byte[] member = key.kind() == RecordKind.LEASE
                ? new byte[0]
                : key.member().orElseThrow().id().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(8 + role.length + member.length);
        buffer.putShort(RECORD_VERSION);
        buffer.putShort(key.kind().code());
        putString(buffer, role);
        putString(buffer, member);
        return buffer.array();
    }

    /**
     * Encodes a record value.
     *
     * <p>A tombstone has no encoded form. Send a null value to write one.
     *
     * @param value the registration or the lease to encode
     * @return the value bytes of the frozen layout
     */
    public static byte[] encodeValue(CoordinationValue value) {
        Objects.requireNonNull(value, "value");
        if (value instanceof Registration registration) {
            byte[] member = registration.member().id().getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(12 + member.length);
            buffer.putShort(RECORD_VERSION);
            putString(buffer, member);
            buffer.putLong(registration.registeredAt());
            return buffer.array();
        }
        Lease lease = (Lease) value;
        byte[] member = lease.member().id().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(30 + member.length);
        buffer.putShort(RECORD_VERSION);
        putString(buffer, member);
        buffer.putLong(lease.token().producerId());
        buffer.putShort(lease.token().producerEpoch());
        buffer.putLong(lease.grantedAt());
        buffer.putLong(lease.deadline());
        return buffer.array();
    }

    /**
     * Decodes a record key.
     *
     * @param bytes the key bytes as they arrived from the broker
     * @return the decoded key
     * @throws CoordinationException if the buffer is truncated, if it holds trailing
     *     bytes, if the version is not {@code 0}, if the kind is neither {@code 0} nor
     *     {@code 1}, if a string length is negative, if a string is not UTF-8, if a
     *     lease key names a member, or if the role or the member breaks its bounds
     * @throws NullPointerException if the key bytes are null
     */
    public static CoordinationKey decodeKey(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        ByteBuffer buffer = ByteBuffer.wrap(bytes.clone());
        try {
            version(buffer, KEY_PART);
            RecordKind kind = RecordKind.fromCode(buffer.getShort());
            String role = string(buffer, KEY_PART);
            String member = string(buffer, KEY_PART);
            finish(buffer, KEY_PART);
            if (kind == RecordKind.LEASE) {
                if (!member.isEmpty()) {
                    throw malformed(KEY_PART,
                            "a lease key carries the empty member string, got \"" + member + "\"");
                }
                return CoordinationKey.lease(Role.of(role));
            }
            return CoordinationKey.registration(Role.of(role), MemberId.of(member));
        } catch (BufferUnderflowException error) {
            throw truncated(KEY_PART, error);
        }
    }

    /**
     * Decodes a record value under the kind of its key.
     *
     * @param kind the kind the key carried
     * @param bytes the value bytes, or null for a tombstone
     * @return the decoded value, and empty for a tombstone
     * @throws CoordinationException if the buffer is truncated, if it holds trailing
     *     bytes, if the version is not {@code 0}, if a string length is negative, if a
     *     string is not UTF-8, if the member breaks its bounds, or if a lease value
     *     carries a negative producer id or producer epoch
     * @throws NullPointerException if the kind is null
     */
    public static Optional<CoordinationValue> decodeValue(RecordKind kind, byte[] bytes) {
        Objects.requireNonNull(kind, "kind");
        if (bytes == null) {
            return Optional.empty();
        }
        String part = kind == RecordKind.REGISTRATION ? REGISTRATION_VALUE_PART : LEASE_VALUE_PART;
        ByteBuffer buffer = ByteBuffer.wrap(bytes.clone());
        try {
            version(buffer, part);
            MemberId member = MemberId.of(string(buffer, part));
            CoordinationValue value;
            if (kind == RecordKind.REGISTRATION) {
                value = new Registration(member, buffer.getLong());
            } else {
                long producerId = buffer.getLong();
                short producerEpoch = buffer.getShort();
                value = new Lease(member, FencingToken.of(producerId, producerEpoch),
                        buffer.getLong(), buffer.getLong());
            }
            finish(buffer, part);
            return Optional.of(value);
        } catch (BufferUnderflowException error) {
            throw truncated(part, error);
        }
    }

    /**
     * Reports whether a record value is a tombstone.
     *
     * <p>No value of this topic is legally empty, so a null value and a zero-length
     * value are both tombstones.
     *
     * @param value the record value bytes, or null
     * @return true when the record clears its key
     */
    public static boolean isTombstone(byte[] value) {
        return value == null || value.length == 0;
    }

    private static void putString(ByteBuffer buffer, byte[] utf8) {
        buffer.putShort((short) utf8.length);
        buffer.put(utf8);
    }

    private static void version(ByteBuffer buffer, String part) {
        short version = buffer.getShort();
        if (version != RECORD_VERSION) {
            throw malformed(part, "unsupported version " + version);
        }
    }

    private static String string(ByteBuffer buffer, String part) {
        short length = buffer.getShort();
        if (length < 0) {
            throw malformed(part, "negative string length " + length);
        }
        if (length > buffer.remaining()) {
            throw truncated(part, null);
        }
        byte[] utf8 = new byte[length];
        buffer.get(utf8);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(utf8))
                    .toString();
        } catch (CharacterCodingException error) {
            throw malformed(part, "a string is not UTF-8");
        }
    }

    private static void finish(ByteBuffer buffer, String part) {
        if (buffer.hasRemaining()) {
            throw malformed(part, "trailing bytes");
        }
    }

    private static CoordinationException malformed(String part, String message) {
        return new CoordinationException("malformed coordination state " + part + ": " + message);
    }

    private static CoordinationException truncated(String part, Throwable cause) {
        return new CoordinationException(
                "malformed coordination state " + part + ": truncated buffer", cause);
    }
}
