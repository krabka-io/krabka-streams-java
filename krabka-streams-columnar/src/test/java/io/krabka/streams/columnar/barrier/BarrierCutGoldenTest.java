package io.krabka.streams.columnar.barrier;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HexFormat;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * The cut wire format is frozen across {@code krabka-broker}, {@code krabka-streams-rs},
 * {@code krabka-streams-go} and this module. These bytes are encoded straight from the layout the
 * broker's barrier modules document, independently of all four implementations, so a decoder that
 * drifts fails here. The same vector is asserted in the other three.
 */
final class BarrierCutGoldenTest {

    /** Version 0, kind 2 (cut), group {@code orders-cut}, epoch 7. */
    private static final String GOLDEN_KEY = "00000002000a6f72646572732d6375740000000000000007";

    /**
     * Version 0, triggered 1724500000000, completed 1724500000042, status 0 (complete), topic
     * {@code orders} with partition 0 at offset 1024 and partition 1 at offset 2048, and no
     * missing partitions.
     */
    private static final String GOLDEN_VALUE =
            "0000000001918435bd00000001918435bd2a000000000100066f7264657273"
                    + "000000020000000000000000000004000000000100000000000008000000"
                    + "0000";

    @Test
    void decodesTheFrozenGoldenBytes() {
        HexFormat hex = HexFormat.of();
        Optional<BarrierCut> decoded =
                BarrierCutDecoder.decode(hex.parseHex(GOLDEN_KEY), hex.parseHex(GOLDEN_VALUE));

        assertThat(decoded).isPresent();
        BarrierCut cut = decoded.orElseThrow();

        assertThat(cut.group()).isEqualTo("orders-cut");
        assertThat(cut.epoch()).isEqualTo(7L);
        assertThat(cut.triggeredAt()).isEqualTo(1_724_500_000_000L);
        assertThat(cut.completedAt()).isEqualTo(1_724_500_000_042L);
        assertThat(cut.complete()).isTrue();
        assertThat(cut.missing()).isEmpty();
        assertThat(cut.offsets())
                .containsOnly(
                        org.assertj.core.api.Assertions.entry(
                                new TopicPartition("orders", 0), 1024L),
                        org.assertj.core.api.Assertions.entry(
                                new TopicPartition("orders", 1), 2048L));
    }
}
