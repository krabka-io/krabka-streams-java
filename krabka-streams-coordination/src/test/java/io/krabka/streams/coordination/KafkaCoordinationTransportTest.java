package io.krabka.streams.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TransactionalIdNotFoundException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

class KafkaCoordinationTransportTest {
    private static final Role ROLE = Role.of("controller");
    private static final MemberId MEMBER = MemberId.of("node-1");
    private static final FencingToken TOKEN = FencingToken.of(4242L, (short) 7);
    private static final TopicPartition STATE_0 = new TopicPartition(CoordinationCodec.TOPIC, 0);

    @Test
    void mapsATransactionDescriptionOntoItsFencingToken() {
        assertThat(KafkaCoordinationTransport.tokenOf(
                        ROLE, KafkaFuture.completedFuture(description(4242L, 7))))
                .contains(TOKEN);
    }

    @Test
    void readsARoleThatNoMemberHasTakenAsNoEpoch() {
        assertThat(KafkaCoordinationTransport.tokenOf(
                        ROLE, KafkaFuture.completedFuture(description(-1L, -1))))
                .isEmpty();
        assertThat(KafkaCoordinationTransport.tokenOf(ROLE, failed(new TransactionalIdNotFoundException("no"))))
                .isEmpty();
    }

    @Test
    void reportsACoordinatorFailureThatIsNotAnUnknownRole() {
        assertThatThrownBy(() ->
                        KafkaCoordinationTransport.tokenOf(ROLE, failed(new TimeoutException("slow"))))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("the transaction coordinator did not describe role controller: slow");
    }

    @Test
    void mintsAnEpochThroughInitTransactionsAndKeepsTheProducerItBinds() {
        MockProducer<byte[], byte[]> transactional = producer();
        AtomicInteger built = new AtomicInteger();
        try (KafkaCoordinationTransport transport = transport(
                role -> Optional.of(TOKEN), producer(), role -> {
                    built.incrementAndGet();
                    return transactional;
                }, consumer(0))) {

            assertThat(transport.acquireEpoch(ROLE)).isEqualTo(TOKEN);

            assertThat(transactional.transactionInitialized()).isTrue();
            assertThat(built.get()).isEqualTo(1);
        }
    }

    @Test
    void buildsASecondProducerForASecondEpochAndClosesTheFirst() {
        // A Kafka producer calls initTransactions once, so a fresh epoch needs a fresh producer.
        List<MockProducer<byte[], byte[]>> built = new ArrayList<>();
        try (KafkaCoordinationTransport transport = transport(
                role -> Optional.of(TOKEN), producer(), role -> {
                    MockProducer<byte[], byte[]> next = producer();
                    built.add(next);
                    return next;
                }, consumer(0))) {

            transport.acquireEpoch(ROLE);
            transport.acquireEpoch(ROLE);

            assertThat(built).hasSize(2);
            assertThat(built.get(0).closed()).isTrue();
            assertThat(built.get(1).closed()).isFalse();
        }
    }

    @Test
    void closesTheProducerWhenInitProducerIdFails() {
        MockProducer<byte[], byte[]> transactional = producer();
        transactional.initTransactionException = new TimeoutException("no coordinator");
        try (KafkaCoordinationTransport transport = transport(
                role -> Optional.of(TOKEN), producer(), role -> transactional, consumer(0))) {

            assertThatThrownBy(() -> transport.acquireEpoch(ROLE))
                    .isInstanceOf(CoordinationException.class)
                    .hasMessage("InitProducerId failed for role controller: no coordinator");
            assertThat(transactional.closed()).isTrue();
        }
    }

    @Test
    void reportsACoordinatorThatMintsNoEpoch() {
        try (KafkaCoordinationTransport transport =
                transport(role -> Optional.empty(), producer(), role -> producer(), consumer(0))) {

            assertThatThrownBy(() -> transport.acquireEpoch(ROLE))
                    .isInstanceOf(CoordinationException.class)
                    .hasMessage("the transaction coordinator reports no transaction for role controller"
                            + " after InitProducerId");
        }
    }

    @Test
    void appendsARegistrationOutsideATransactionAndOnThePartitionOfTheRole() {
        MockProducer<byte[], byte[]> registrar = producer();
        try (KafkaCoordinationTransport transport =
                transport(role -> Optional.of(TOKEN), registrar, role -> producer(), consumer(0))) {

            transport.register(ROLE, MEMBER, 1_700_000_000_000L);

            assertThat(registrar.transactionInFlight()).isFalse();
            assertThat(registrar.history()).hasSize(1);
            ProducerRecord<byte[], byte[]> record = registrar.history().get(0);
            assertThat(record.topic()).isEqualTo(CoordinationCodec.TOPIC);
            assertThat(record.partition()).isEqualTo(0);
            assertThat(CoordinationCodec.decodeKey(record.key()))
                    .usingRecursiveComparison()
                    .isEqualTo(CoordinationKey.registration(ROLE, MEMBER));
            assertThat(CoordinationCodec.decodeValue(RecordKind.REGISTRATION, record.value()))
                    .usingRecursiveComparison()
                    .isEqualTo(Optional.of(new Registration(MEMBER, 1_700_000_000_000L)));
        }
    }

    @Test
    void writesALeaseInsideATransactionUnderTheTokenItMinted() {
        MockProducer<byte[], byte[]> transactional = producer();
        try (KafkaCoordinationTransport transport = transport(
                role -> Optional.of(TOKEN), producer(), role -> transactional, consumer(0))) {
            transport.acquireEpoch(ROLE);
            Lease lease = new Lease(MEMBER, TOKEN, 1_700_000_000_000L, 1_700_000_030_000L);

            transport.writeLease(ROLE, TOKEN, lease);

            assertThat(transactional.transactionCommitted()).isTrue();
            assertThat(transactional.history()).hasSize(1);
            ProducerRecord<byte[], byte[]> record = transactional.history().get(0);
            assertThat(CoordinationCodec.decodeKey(record.key()))
                    .usingRecursiveComparison()
                    .isEqualTo(CoordinationKey.lease(ROLE));
            assertThat(CoordinationCodec.decodeValue(RecordKind.LEASE, record.value()))
                    .usingRecursiveComparison()
                    .isEqualTo(Optional.of(lease));
        }
    }

    @Test
    void clearsALeaseWithATombstone() {
        MockProducer<byte[], byte[]> transactional = producer();
        try (KafkaCoordinationTransport transport = transport(
                role -> Optional.of(TOKEN), producer(), role -> transactional, consumer(0))) {
            transport.acquireEpoch(ROLE);

            transport.clearLease(ROLE, TOKEN);

            assertThat(transactional.history().get(0).value()).isNull();
        }
    }

    @Test
    void reportsAFenceOnALeaseWriteAsTheLossOfTheRole() {
        MockProducer<byte[], byte[]> transactional = producer();
        try (KafkaCoordinationTransport transport = transport(
                role -> Optional.of(TOKEN), producer(), role -> transactional, consumer(0))) {
            transport.acquireEpoch(ROLE);
            transactional.fenceProducer();

            assertThatThrownBy(() -> transport.writeLease(
                            ROLE, TOKEN, new Lease(MEMBER, TOKEN, 0L, 30_000L)))
                    .isInstanceOf(FencedException.class)
                    .hasMessage("fenced: another member holds role controller")
                    .hasCauseInstanceOf(ProducerFencedException.class);
        }
    }

    @Test
    void refusesAWriteUnderATokenItNeverMinted() {
        try (KafkaCoordinationTransport transport =
                transport(role -> Optional.of(TOKEN), producer(), role -> producer(), consumer(0))) {

            assertThatThrownBy(() -> transport.writeLease(
                            ROLE, TOKEN, new Lease(MEMBER, TOKEN, 0L, 30_000L)))
                    .isInstanceOf(CoordinationException.class)
                    .hasMessage("this transport did not mint token 4242:7 for role controller");
        }
    }

    @Test
    void refusesAWriteUnderASupersededToken() {
        MockProducer<byte[], byte[]> transactional = producer();
        try (KafkaCoordinationTransport transport = transport(
                role -> Optional.of(TOKEN), producer(), role -> transactional, consumer(0))) {
            transport.acquireEpoch(ROLE);
            FencingToken stale = FencingToken.of(4242L, (short) 6);

            assertThatThrownBy(() -> transport.writeLease(
                            ROLE, stale, new Lease(MEMBER, stale, 0L, 30_000L)))
                    .isInstanceOf(CoordinationException.class)
                    .hasMessage("this transport did not mint token 4242:6 for role controller");
        }
    }

    @Test
    void readsTheWholePartitionOfARoleInOffsetOrder() {
        MockConsumer<byte[], byte[]> consumer = consumer(0);
        seed(
                consumer,
                registrationRecord(0, ROLE, MEMBER, 100L),
                leaseRecord(1, ROLE, MEMBER, 100L, 30_100L),
                tombstoneRecord(2, CoordinationKey.lease(ROLE)));
        try (KafkaCoordinationTransport transport =
                transport(role -> Optional.of(TOKEN), producer(), role -> producer(), consumer)) {

            assertThat(transport.readRoleRecords(ROLE))
                    .usingRecursiveComparison()
                    .isEqualTo(List.of(
                            new CoordinationEntry(
                                    0,
                                    CoordinationKey.registration(ROLE, MEMBER),
                                    Optional.of(new Registration(MEMBER, 100L))),
                            new CoordinationEntry(
                                    1,
                                    CoordinationKey.lease(ROLE),
                                    Optional.of(new Lease(MEMBER, TOKEN, 100L, 30_100L))),
                            new CoordinationEntry(2, CoordinationKey.lease(ROLE), Optional.empty())));
        }
    }

    @Test
    void dropsEveryRecordOfAnotherRoleThatSharesThePartition() {
        MockConsumer<byte[], byte[]> consumer = consumer(0);
        seed(
                consumer,
                registrationRecord(0, Role.of("compactor"), MEMBER, 50L),
                registrationRecord(1, ROLE, MEMBER, 100L));
        try (KafkaCoordinationTransport transport =
                transport(role -> Optional.of(TOKEN), producer(), role -> producer(), consumer)) {

            assertThat(transport.readRoleRecords(ROLE))
                    .singleElement()
                    .extracting(entry -> entry.key().role())
                    .isEqualTo(ROLE);
        }
    }

    @Test
    void reportsAMissingCoordinationTopic() {
        try (MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
                KafkaCoordinationTransport transport =
                        transport(role -> Optional.of(TOKEN), producer(), role -> producer(), consumer)) {

            assertThatThrownBy(() -> transport.readRoleRecords(ROLE))
                    .isInstanceOf(CoordinationException.class)
                    .hasMessage("topic __coordination_state has no partitions");
        }
    }

    @Test
    void reportsARecordWithNoKey() {
        MockConsumer<byte[], byte[]> consumer = consumer(0);
        seed(consumer, new ConsumerRecord<>(CoordinationCodec.TOPIC, 0, 0L, null, new byte[] {0}));
        try (KafkaCoordinationTransport transport =
                transport(role -> Optional.of(TOKEN), producer(), role -> producer(), consumer)) {

            assertThatThrownBy(() -> transport.readRoleRecords(ROLE))
                    .isInstanceOf(CoordinationException.class)
                    .hasMessage("a record of __coordination_state at offset 0 carries no key");
        }
    }

    @Test
    void closesTheProducersItBuiltAndLeavesTheCallerClientsOpen() {
        MockProducer<byte[], byte[]> registrar = producer();
        MockProducer<byte[], byte[]> transactional = producer();
        MockConsumer<byte[], byte[]> consumer = consumer(0);
        KafkaCoordinationTransport transport =
                transport(role -> Optional.of(TOKEN), registrar, role -> transactional, consumer);
        transport.acquireEpoch(ROLE);

        transport.close();

        assertThat(transactional.closed()).isTrue();
        assertThat(registrar.closed()).isFalse();
        consumer.close();
    }

    private static KafkaCoordinationTransport transport(
            Function<Role, Optional<FencingToken>> epochReader,
            Producer<byte[], byte[]> registrar,
            Function<Role, Producer<byte[], byte[]>> transactionalProducers,
            MockConsumer<byte[], byte[]> consumer) {
        return new KafkaCoordinationTransport(
                epochReader, registrar, transactionalProducers, consumer, Duration.ZERO);
    }

    private static MockProducer<byte[], byte[]> producer() {
        return new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static MockConsumer<byte[], byte[]> consumer(int partition) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        consumer.updatePartitions(
                CoordinationCodec.TOPIC,
                List.of(new PartitionInfo(
                        CoordinationCodec.TOPIC, partition, null, new Node[0], new Node[0])));
        return consumer;
    }

    @SafeVarargs
    private static void seed(
            MockConsumer<byte[], byte[]> consumer, ConsumerRecord<byte[], byte[]>... records) {
        consumer.assign(List.of(STATE_0));
        consumer.updateBeginningOffsets(Map.of(STATE_0, 0L));
        List<ConsumerRecord<byte[], byte[]>> added = new ArrayList<>(List.of(records));
        added.forEach(consumer::addRecord);
        consumer.updateEndOffsets(Map.of(STATE_0, (long) added.size()));
    }

    private static ConsumerRecord<byte[], byte[]> registrationRecord(
            long offset, Role role, MemberId member, long registeredAt) {
        return new ConsumerRecord<>(
                CoordinationCodec.TOPIC,
                0,
                offset,
                CoordinationCodec.encodeKey(CoordinationKey.registration(role, member)),
                CoordinationCodec.encodeValue(new Registration(member, registeredAt)));
    }

    private static ConsumerRecord<byte[], byte[]> leaseRecord(
            long offset, Role role, MemberId member, long grantedAt, long deadline) {
        return new ConsumerRecord<>(
                CoordinationCodec.TOPIC,
                0,
                offset,
                CoordinationCodec.encodeKey(CoordinationKey.lease(role)),
                CoordinationCodec.encodeValue(new Lease(member, TOKEN, grantedAt, deadline)));
    }

    private static ConsumerRecord<byte[], byte[]> tombstoneRecord(long offset, CoordinationKey key) {
        return new ConsumerRecord<>(
                CoordinationCodec.TOPIC, 0, offset, CoordinationCodec.encodeKey(key), null);
    }

    private static TransactionDescription description(long producerId, int producerEpoch) {
        return new TransactionDescription(
                0,
                TransactionState.ONGOING,
                producerId,
                producerEpoch,
                60_000L,
                java.util.OptionalLong.empty(),
                java.util.Set.of());
    }

    private static KafkaFuture<TransactionDescription> failed(Throwable cause) {
        KafkaFutureImpl<TransactionDescription> future = new KafkaFutureImpl<>();
        future.completeExceptionally(cause);
        return future;
    }
}
