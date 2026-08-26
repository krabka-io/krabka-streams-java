package io.krabka.streams.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FencingTokenTest {
    @Test
    void ranksAFreshProducerIdAboveAWrappedEpoch() {
        // The producer epoch is a short and it wraps. Kafka then allocates a new producer id and
        // resets the epoch to zero. A comparison on the epoch alone would keep the old leader.
        FencingToken wrapped = FencingToken.of(4L, Short.MAX_VALUE);
        FencingToken fresh = FencingToken.of(5L, (short) 0);

        assertThat(fresh.supersedes(wrapped)).isTrue();
        assertThat(wrapped.supersedes(fresh)).isFalse();
        assertThat(fresh).isGreaterThan(wrapped);
    }

    @Test
    void ordersTwoTokensOfOneProducerIdByTheirEpoch() {
        assertThat(FencingToken.of(7L, (short) 2)).isGreaterThan(FencingToken.of(7L, (short) 1));
        assertThat(FencingToken.of(7L, (short) 1)).isEqualByComparingTo(FencingToken.of(7L, (short) 1));
    }

    @Test
    void sortsLexicographicallyByProducerIdAndThenByEpoch() {
        List<FencingToken> sorted = List.of(
                        FencingToken.of(5L, (short) 0),
                        FencingToken.of(4L, Short.MAX_VALUE),
                        FencingToken.of(4L, (short) 0),
                        FencingToken.NO_EPOCH)
                .stream()
                .sorted()
                .toList();

        assertThat(sorted)
                .containsExactly(
                        FencingToken.NO_EPOCH,
                        FencingToken.of(4L, (short) 0),
                        FencingToken.of(4L, Short.MAX_VALUE),
                        FencingToken.of(5L, (short) 0));
    }

    @Test
    void ranksTheNoEpochSentinelBelowEveryMintedToken() {
        assertThat(FencingToken.of(0L, (short) 0).supersedes(FencingToken.NO_EPOCH)).isTrue();
        assertThat(FencingToken.NO_EPOCH.minted()).isFalse();
        assertThat(FencingToken.of(0L, (short) 0).minted()).isTrue();
    }

    @Test
    void rejectsANegativeProducerIdOrEpoch() {
        assertThatThrownBy(() -> FencingToken.of(-1L, (short) 0))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("a fencing token must not be negative, got -1:0");
        assertThatThrownBy(() -> FencingToken.of(0L, (short) -1))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("a fencing token must not be negative, got 0:-1");
    }

    @Test
    void roundTripsThroughItsTextForm() {
        FencingToken token = FencingToken.of(4242L, (short) 7);

        assertThat(token).hasToString("4242:7");
        assertThat(FencingToken.parse(token.toString())).isEqualTo(token);
    }

    @ParameterizedTest
    @ValueSource(strings = {"4242", "4242:7:1", "4242:", ":7", "four:7", "4242:seven", "4242:40000", ""})
    void rejectsATextThatIsNotAToken(String text) {
        assertThatThrownBy(() -> FencingToken.parse(text)).isInstanceOf(CoordinationException.class);
    }

    @Test
    void treatsTwoTokensOfOnePairAsEqual() {
        assertThat(FencingToken.of(1L, (short) 2))
                .isEqualTo(FencingToken.of(1L, (short) 2))
                .hasSameHashCodeAs(FencingToken.of(1L, (short) 2))
                .isNotEqualTo(FencingToken.of(1L, (short) 3))
                .isNotEqualTo("1:2");
    }
}
