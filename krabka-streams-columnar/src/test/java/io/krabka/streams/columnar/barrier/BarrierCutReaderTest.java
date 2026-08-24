package io.krabka.streams.columnar.barrier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.krabka.streams.columnar.ColumnarException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class BarrierCutReaderTest {
    private static final TopicPartition ORDERS_0 = new TopicPartition("orders", 0);
    private static final TopicPartition ORDERS_1 = new TopicPartition("orders", 1);
    private static final TopicPartition STATE_0 = new TopicPartition(BarrierCutDecoder.TOPIC, 0);

    @Test
    void readsCompleteCutsAndSkipsEveryOtherRecordKind() {
        try (var consumer = new MockConsumer<byte[], byte[]>("earliest")) {
            seed(
                    consumer,
                    record(0, BarrierTestRecords.key(BarrierTestRecords.GROUP_KIND, "ledger", -1), new byte[] {0}),
                    record(
                            1,
                            BarrierTestRecords.key(BarrierTestRecords.INJECTION_START_KIND, "ledger", 4),
                            new byte[] {0}),
                    cut(2, "ledger", 4, BarrierCutStatus.COMPLETE, Map.of(ORDERS_0, 10L, ORDERS_1, 20L)),
                    cut(3, "ledger", 5, BarrierCutStatus.COMPLETE, Map.of(ORDERS_0, 30L, ORDERS_1, 40L)),
                    record(4, BarrierTestRecords.key(BarrierTestRecords.GROUP_KIND, "ledger", -1), null));
            var reader = new BarrierCutReader(consumer, Duration.ZERO);

            assertThat(reader.latestCompleteCut("ledger"))
                    .get()
                    .usingRecursiveComparison()
                    .isEqualTo(expected("ledger", 5, Map.of(ORDERS_0, 30L, ORDERS_1, 40L)));
            assertThat(reader.completeCutsAfter("ledger", -1))
                    .usingRecursiveComparison()
                    .isEqualTo(List.of(
                            expected("ledger", 4, Map.of(ORDERS_0, 10L, ORDERS_1, 20L)),
                            expected("ledger", 5, Map.of(ORDERS_0, 30L, ORDERS_1, 40L))));
            assertThat(reader.completeCutsAfter("ledger", 4))
                    .usingRecursiveComparison()
                    .isEqualTo(List.of(expected("ledger", 5, Map.of(ORDERS_0, 30L, ORDERS_1, 40L))));
        }
    }

    @Test
    void neverReturnsAPartialCutAsAlignable() {
        try (var consumer = new MockConsumer<byte[], byte[]>("earliest")) {
            seed(
                    consumer,
                    cut(0, "ledger", 4, BarrierCutStatus.COMPLETE, Map.of(ORDERS_0, 10L)),
                    cut(1, "ledger", 5, BarrierCutStatus.PARTIAL, Map.of(ORDERS_0, 30L)));
            var reader = new BarrierCutReader(consumer, Duration.ZERO);

            assertThat(reader.completeCutsAfter("ledger", -1))
                    .usingRecursiveComparison()
                    .isEqualTo(List.of(expected("ledger", 4, Map.of(ORDERS_0, 10L))));
            assertThat(reader.latestCompleteCut("ledger"))
                    .get()
                    .usingRecursiveComparison()
                    .isEqualTo(expected("ledger", 4, Map.of(ORDERS_0, 10L)));
        }
    }

    @Test
    void separatesGroupsAndReportsAnUnknownGroupAsEmpty() {
        try (var consumer = new MockConsumer<byte[], byte[]>("earliest")) {
            seed(
                    consumer,
                    cut(0, "ledger", 1, BarrierCutStatus.COMPLETE, Map.of(ORDERS_0, 10L)),
                    cut(1, "audit", 1, BarrierCutStatus.COMPLETE, Map.of(ORDERS_1, 70L)));
            var reader = new BarrierCutReader(consumer, Duration.ZERO);

            assertThat(reader.latestCompleteCut("audit"))
                    .get()
                    .usingRecursiveComparison()
                    .isEqualTo(expected("audit", 1, Map.of(ORDERS_1, 70L)));
            assertThat(reader.latestCompleteCut("unknown")).isEmpty();
            assertThat(reader.completeCutsAfter("unknown", -1)).isEmpty();
        }
    }

    @Test
    void picksUpCutsThatArriveAfterAnEarlierRead() {
        try (var consumer = new MockConsumer<byte[], byte[]>("earliest")) {
            seed(consumer, cut(0, "ledger", 1, BarrierCutStatus.COMPLETE, Map.of(ORDERS_0, 10L)));
            var reader = new BarrierCutReader(consumer, Duration.ZERO);
            assertThat(reader.completeCutsAfter("ledger", -1)).hasSize(1);

            consumer.addRecord(cut(1, "ledger", 2, BarrierCutStatus.COMPLETE, Map.of(ORDERS_0, 20L)));
            consumer.updateEndOffsets(Map.of(STATE_0, 2L));

            assertThat(reader.latestCompleteCut("ledger"))
                    .get()
                    .usingRecursiveComparison()
                    .isEqualTo(expected("ledger", 2, Map.of(ORDERS_0, 20L)));
        }
    }

    @Test
    void reportsAMissingBarrierStateTopic() {
        try (var consumer = new MockConsumer<byte[], byte[]>("earliest")) {
            var reader = new BarrierCutReader(consumer, Duration.ZERO);

            assertThatThrownBy(() -> reader.latestCompleteCut("ledger"))
                    .isInstanceOf(ColumnarException.class)
                    .hasMessage("barrier state topic __barrier_state has no partitions");
        }
    }

    @SafeVarargs
    private static void seed(MockConsumer<byte[], byte[]> consumer, ConsumerRecord<byte[], byte[]>... records) {
        consumer.updatePartitions(
                BarrierCutDecoder.TOPIC,
                List.of(new PartitionInfo(BarrierCutDecoder.TOPIC, 0, null, new Node[0], new Node[0])));
        consumer.assign(List.of(STATE_0));
        consumer.updateBeginningOffsets(Map.of(STATE_0, 0L));
        for (var record : records) {
            consumer.addRecord(record);
        }
        consumer.updateEndOffsets(Map.of(STATE_0, (long) records.length));
    }

    private static ConsumerRecord<byte[], byte[]> record(long offset, byte[] key, byte[] value) {
        return new ConsumerRecord<>(BarrierCutDecoder.TOPIC, 0, offset, key, value);
    }

    private static ConsumerRecord<byte[], byte[]> cut(
            long offset, String group, long epoch, BarrierCutStatus status, Map<TopicPartition, Long> offsets) {
        return record(
                offset,
                BarrierTestRecords.key(BarrierTestRecords.CUT_KIND, group, epoch),
                BarrierTestRecords.cutValue(epoch * 10, epoch * 10 + 1, status, offsets, List.of()));
    }

    private static BarrierCut expected(String group, long epoch, Map<TopicPartition, Long> offsets) {
        return new BarrierCut(
                group, epoch, epoch * 10, epoch * 10 + 1, BarrierCutStatus.COMPLETE, offsets, Set.of());
    }
}
