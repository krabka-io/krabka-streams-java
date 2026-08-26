package io.krabka.streams.coordination;

/**
 * One decoded value of {@link CoordinationCodec#TOPIC}.
 *
 * <p>The interface has two implementations, and the kind of the key picks which one a
 * value decodes to. {@link Registration} is the value of a registration key, and
 * {@link Lease} is the value of a lease key. A record with a null value is a tombstone,
 * and {@link CoordinationCodec#decodeValue(RecordKind, byte[])} returns an empty
 * {@link java.util.Optional} for it rather than a third implementation.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * Optional<CoordinationValue> value =
 *     CoordinationCodec.decodeValue(key.kind(), record.value());
 * if (value.orElse(null) instanceof Lease lease) {
 *     System.out.println(lease.member() + " holds the role until " + lease.deadline());
 * }
 * }</pre>
 */
public sealed interface CoordinationValue permits Registration, Lease {
    /**
     * Returns the record kind whose key this value belongs under.
     *
     * @return {@link RecordKind#REGISTRATION} for a registration and
     *     {@link RecordKind#LEASE} for a lease
     */
    RecordKind kind();

    /**
     * Returns the member the value names.
     *
     * @return the member that registered, or the member that holds the role
     */
    MemberId member();
}
