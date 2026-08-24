package io.krabka.streams.columnar.barrier;

import io.krabka.streams.columnar.ColumnarException;

/**
 * Whether a barrier coordinator reached every partition of a cut.
 *
 * <p>The coordinator publishes both outcomes to {@code __barrier_state}. A complete
 * cut has a marker offset for every partition of the barrier group. A partial cut has
 * partitions that never received a marker, so no task can align on it: the missing
 * partitions would never reach the cut. {@link BarrierCutReader} returns complete
 * cuts only.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * if (cut.status() == BarrierCutStatus.PARTIAL) {
 *     log.warn("epoch {} missed {}", cut.epoch(), cut.missing());
 * }
 * }</pre>
 */
public enum BarrierCutStatus {
    /** Every partition of the barrier group received the epoch's marker. */
    COMPLETE(0),

    /** One or more partitions never received the epoch's marker. */
    PARTIAL(1);

    private final int code;

    BarrierCutStatus(int code) {
        this.code = code;
    }

    /**
     * Returns the wire code the coordinator writes for this status.
     *
     * @return {@code 0} for {@link #COMPLETE} and {@code 1} for {@link #PARTIAL}
     */
    public int code() {
        return code;
    }

    /**
     * Maps a wire code onto a status.
     *
     * @param code the status byte read from a cut record
     * @return the matching status
     * @throws ColumnarException if no status carries the code
     */
    public static BarrierCutStatus fromCode(int code) {
        for (var status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new ColumnarException("unknown barrier cut status " + code);
    }
}
