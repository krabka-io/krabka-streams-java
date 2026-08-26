package io.krabka.streams.coordination;

/**
 * The quorum-minted proof that one member holds a role.
 *
 * <p>Kafka's transaction coordinator mints the pair when a member calls
 * {@code InitProducerId} for the {@code transactional.id} of the role. The quorum picks
 * the values, every broker enforces them, and a write from an older pair fails with a
 * fenced error. A holder passes the token to every guarded write, and the broker checks
 * it. No clock takes part in that check.
 *
 * <p>The token is a pair, and {@link #compareTo(FencingToken)} reads the producer id
 * first and the producer epoch second. The order is lexicographic. A producer epoch is
 * a {@code short} and it wraps. Kafka handles the exhaustion with a fresh producer id
 * and an epoch of zero. A comparison on the epoch alone ranks that fresh token below
 * the stale one, and the old leader keeps the role after about 32000 leadership
 * changes. Never compare the epoch on its own.
 *
 * <p>{@link #NO_EPOCH} is the token of a role that no member has ever taken. It ranks
 * below every minted token.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * FencingToken wrapped = FencingToken.of(4, Short.MAX_VALUE);
 * FencingToken fresh = FencingToken.of(5, (short) 0);
 * assertThat(fresh.supersedes(wrapped)).isTrue();
 * }</pre>
 */
public final class FencingToken implements Comparable<FencingToken> {
    /**
     * The token of a role that no member has ever taken.
     *
     * <p>Kafka writes {@code -1} for "no producer", and that pair proves no leadership.
     * {@link #of(long, short)} rejects a negative value, so no minted token can equal
     * this one and every minted token supersedes it.
     */
    public static final FencingToken NO_EPOCH = new FencingToken(-1L, (short) -1);

    private final long producerId;
    private final short producerEpoch;

    private FencingToken(long producerId, short producerEpoch) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
    }

    /**
     * Builds a token from a minted producer id and producer epoch.
     *
     * @param producerId the producer id the transaction coordinator minted
     * @param producerEpoch the producer epoch the transaction coordinator minted
     * @return the fencing token of the pair
     * @throws CoordinationException if either value is negative
     */
    public static FencingToken of(long producerId, short producerEpoch) {
        if (producerId < 0 || producerEpoch < 0) {
            throw new CoordinationException("a fencing token must not be negative, got "
                    + producerId + ":" + producerEpoch);
        }
        return new FencingToken(producerId, producerEpoch);
    }

    /**
     * Parses the {@code producer_id:producer_epoch} form that {@link #toString()}
     * writes.
     *
     * @param text the two decimal numbers, separated by one colon
     * @return the fencing token the text names
     * @throws CoordinationException if the text does not carry that form, or if either
     *     number is negative or out of range
     */
    public static FencingToken parse(String text) {
        int colon = text.indexOf(':');
        if (colon < 0 || text.indexOf(':', colon + 1) >= 0) {
            throw malformed(text);
        }
        try {
            return of(Long.parseLong(text.substring(0, colon)),
                    Short.parseShort(text.substring(colon + 1)));
        } catch (NumberFormatException error) {
            throw malformed(text);
        }
    }

    private static CoordinationException malformed(String text) {
        return new CoordinationException(
                "expected a fencing token of the form producer_id:producer_epoch, got \""
                        + text + "\"");
    }

    /**
     * Returns the producer id the transaction coordinator minted.
     *
     * @return the producer id, or {@code -1} for {@link #NO_EPOCH}
     */
    public long producerId() {
        return producerId;
    }

    /**
     * Returns the producer epoch the transaction coordinator minted.
     *
     * @return the producer epoch, or {@code -1} for {@link #NO_EPOCH}
     */
    public short producerEpoch() {
        return producerEpoch;
    }

    /**
     * Reports whether this token names a minted leadership.
     *
     * @return true for every token but {@link #NO_EPOCH}
     */
    public boolean minted() {
        return producerId >= 0;
    }

    /**
     * Orders two tokens lexicographically.
     *
     * <p>The comparison reads the producer id first, and it reads the producer epoch
     * only for two tokens of one producer id. Kafka allocates producer ids from a
     * monotonic block allocator and resets the epoch to zero at every new id, so the
     * pair stays monotonic where the epoch alone does not.
     *
     * @param other the token to compare against
     * @return a negative number when this token ranks below the other one, zero for two
     *     equal tokens, and a positive number when this token supersedes the other one
     */
    @Override
    public int compareTo(FencingToken other) {
        int order = Long.compare(producerId, other.producerId);
        return order != 0 ? order : Short.compare(producerEpoch, other.producerEpoch);
    }

    /**
     * Reports whether this token ranks above another one.
     *
     * @param other the token to compare against
     * @return true when this token supersedes the other one
     */
    public boolean supersedes(FencingToken other) {
        return compareTo(other) > 0;
    }

    /**
     * Reports whether another object is a token with the same pair.
     *
     * @param other the object to compare against
     * @return true when the other object is a token of this producer id and epoch
     */
    @Override
    public boolean equals(Object other) {
        return other instanceof FencingToken token
                && producerId == token.producerId
                && producerEpoch == token.producerEpoch;
    }

    /**
     * Returns a hash of the pair.
     *
     * @return the hash code of the producer id and the producer epoch
     */
    @Override
    public int hashCode() {
        return Long.hashCode(producerId) * 31 + producerEpoch;
    }

    /**
     * Returns the {@code producer_id:producer_epoch} form.
     *
     * @return the two decimal numbers, separated by one colon
     */
    @Override
    public String toString() {
        return producerId + ":" + producerEpoch;
    }
}
