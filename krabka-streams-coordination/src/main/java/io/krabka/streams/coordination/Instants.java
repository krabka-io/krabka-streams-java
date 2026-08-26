package io.krabka.streams.coordination;

/** Saturating arithmetic on the epoch-millisecond line. */
final class Instants {
    private Instants() {
    }

    /**
     * Adds an extent to an instant and saturates instead of wrapping.
     *
     * @param instantMillis the instant, in milliseconds since the Unix epoch
     * @param extentMillis the extent to add, in milliseconds
     * @return the later instant, capped at {@link Long#MAX_VALUE}
     */
    static long add(long instantMillis, long extentMillis) {
        long sum = instantMillis + extentMillis;
        if (((instantMillis ^ sum) & (extentMillis ^ sum)) < 0) {
            return extentMillis > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
        return sum;
    }

    /**
     * Multiplies an extent by a rank and saturates instead of wrapping.
     *
     * @param extentMillis the extent of one rank, in milliseconds
     * @param rank the number of ranks
     * @return the product, capped at {@link Long#MAX_VALUE}
     */
    static long multiply(long extentMillis, int rank) {
        if (rank <= 0) {
            return 0L;
        }
        long product = extentMillis * rank;
        if (extentMillis != 0 && (product / extentMillis != rank || product < 0)) {
            return Long.MAX_VALUE;
        }
        return product;
    }
}
