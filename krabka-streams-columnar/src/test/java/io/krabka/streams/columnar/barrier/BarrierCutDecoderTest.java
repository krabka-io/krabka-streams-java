package io.krabka.streams.columnar.barrier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.testing.junit.testparameterinjector.junit5.TestParameter;
import com.google.testing.junit.testparameterinjector.junit5.TestParameterInjectorTest;
import io.krabka.streams.columnar.ColumnarException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class BarrierCutDecoderTest {
    private static final TopicPartition ORDERS_0 = new TopicPartition("orders", 0);
    private static final TopicPartition ORDERS_1 = new TopicPartition("orders", 1);
    private static final TopicPartition PAYMENTS_0 = new TopicPartition("payments", 0);

    @Test
    void decodesACompleteCutFromTheFrozenLayout() {
        var key = BarrierTestRecords.key(BarrierTestRecords.CUT_KIND, "ledger", 7);
        var value = BarrierTestRecords.cutValue(
                1_700_000_000_000L,
                1_700_000_000_250L,
                BarrierCutStatus.COMPLETE,
                Map.of(ORDERS_0, 12L, ORDERS_1, 34L, PAYMENTS_0, 5L),
                List.of());

        assertThat(BarrierCutDecoder.decode(key, value))
                .get()
                .usingRecursiveComparison()
                .isEqualTo(new BarrierCut(
                        "ledger",
                        7,
                        1_700_000_000_000L,
                        1_700_000_000_250L,
                        BarrierCutStatus.COMPLETE,
                        Map.of(ORDERS_0, 12L, ORDERS_1, 34L, PAYMENTS_0, 5L),
                        Set.of()));
    }

    @Test
    void decodesAPartialCutWithItsMissingPartitions() {
        var key = BarrierTestRecords.key(BarrierTestRecords.CUT_KIND, "ledger", 8);
        var value = BarrierTestRecords.cutValue(
                20L, 30L, BarrierCutStatus.PARTIAL, Map.of(ORDERS_0, 12L), List.of(ORDERS_1, PAYMENTS_0));

        assertThat(BarrierCutDecoder.decode(key, value))
                .get()
                .usingRecursiveComparison()
                .isEqualTo(new BarrierCut(
                        "ledger",
                        8,
                        20L,
                        30L,
                        BarrierCutStatus.PARTIAL,
                        Map.of(ORDERS_0, 12L),
                        Set.of(ORDERS_1, PAYMENTS_0)));
    }

    @TestParameterInjectorTest
    void skipsEveryRecordThatIsNotACut(@TestParameter({"0", "1"}) int kind) {
        var key = BarrierTestRecords.key(kind, "ledger", kind == 0 ? -1 : 9);
        var value = BarrierTestRecords.cutValue(
                1L, 2L, BarrierCutStatus.COMPLETE, Map.of(ORDERS_0, 3L), List.of());

        assertThat(BarrierCutDecoder.decode(key, value)).isEmpty();
    }

    @Test
    void skipsATombstoneAndAKeylessRecord() {
        var key = BarrierTestRecords.key(BarrierTestRecords.CUT_KIND, "ledger", 7);

        assertThat(BarrierCutDecoder.decode(key, null)).isEmpty();
        assertThat(BarrierCutDecoder.decode(null, new byte[0])).isEmpty();
    }

    @TestParameterInjectorTest
    void rejectsMalformedRecords(@TestParameter Malformed malformed) {
        assertThatThrownBy(() -> BarrierCutDecoder.decode(malformed.key(), malformed.value()))
                .isInstanceOf(ColumnarException.class)
                .hasMessage(malformed.message);
    }

    enum Malformed {
        KEY_VERSION("unsupported barrier record version 1") {
            @Override
            byte[] key() {
                var key = validKey();
                key[1] = 1;
                return key;
            }
        },
        TRUNCATED_KEY("truncated barrier cut record") {
            @Override
            byte[] key() {
                return Arrays.copyOf(validKey(), 6);
            }
        },
        TRAILING_KEY_BYTES("trailing bytes in barrier cut key") {
            @Override
            byte[] key() {
                return Arrays.copyOf(validKey(), validKey().length + 1);
            }
        },
        NEGATIVE_GROUP_LENGTH("negative barrier string length -1") {
            @Override
            byte[] key() {
                var key = validKey();
                key[4] = (byte) 0xFF;
                key[5] = (byte) 0xFF;
                return key;
            }
        },
        VALUE_VERSION("unsupported barrier cut version 3") {
            @Override
            byte[] value() {
                var value = validValue();
                value[1] = 3;
                return value;
            }
        },
        UNKNOWN_STATUS("unknown barrier cut status 9") {
            @Override
            byte[] value() {
                var value = validValue();
                value[18] = 9;
                return value;
            }
        },
        TRUNCATED_VALUE("truncated barrier cut record") {
            @Override
            byte[] value() {
                return Arrays.copyOf(validValue(), validValue().length - 4);
            }
        },
        TRAILING_VALUE_BYTES("trailing bytes in barrier cut record") {
            @Override
            byte[] value() {
                return Arrays.copyOf(validValue(), validValue().length + 1);
            }
        },
        NEGATIVE_TOPIC_COUNT("negative barrier cut topic count") {
            @Override
            byte[] value() {
                var value = validValue();
                value[19] = (byte) 0xFF;
                value[20] = (byte) 0xFF;
                value[21] = (byte) 0xFF;
                value[22] = (byte) 0xFF;
                return value;
            }
        };

        private final String message;

        Malformed(String message) {
            this.message = message;
        }

        byte[] key() {
            return validKey();
        }

        byte[] value() {
            return validValue();
        }

        static byte[] validKey() {
            return BarrierTestRecords.key(BarrierTestRecords.CUT_KIND, "ledger", 7);
        }

        static byte[] validValue() {
            return BarrierTestRecords.cutValue(
                    1L, 2L, BarrierCutStatus.COMPLETE, Map.of(ORDERS_0, 3L), List.of());
        }
    }
}
