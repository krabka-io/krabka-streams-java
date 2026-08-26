package io.krabka.streams.coordination;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TransactionalIdNotFoundException;

/**
 * The {@link CoordinationTransport} that talks to a Kafka cluster.
 *
 * <p>The transport composes an {@link Admin} client, two kinds of producer, and one
 * consumer. It adds no wire message. The points below are the parts of the composition
 * that a reader cannot see from the call sites, and each one is load-bearing.
 *
 * <h2>One role uses two producers</h2>
 *
 * <p>The transactional producer carries {@code transactional.id = <role>}, and it writes
 * the lease records of that role. A second, plain producer appends the registration
 * records of every role. Kafka puts every send of a transactional producer inside a
 * transaction, and a candidate holds no epoch to open one with, so a registration
 * cannot travel on the transactional producer.
 *
 * <h2>The transactional producer is the epoch</h2>
 *
 * <p>{@link #acquireEpoch(Role)} builds the transactional producer of the role and calls
 * {@code initTransactions}. That call sends {@code InitProducerId} to the transaction
 * coordinator, and the coordinator mints the producer epoch of the role. The transport
 * keeps the producer, because the same instance stays bound to the epoch that the
 * coordinator minted for it, and every write it makes carries that epoch. The Java
 * producer reports no epoch of its own, so the transport reads the minted pair back
 * with {@code DescribeTransactions}. A member that another member overtakes between the
 * two calls reads the newer pair, and its first lease write then fails with
 * {@link FencedException}. That is the same signal the design gives every loser.
 *
 * <h2>A fence is the loss of the role</h2>
 *
 * <p>{@link #writeLease(Role, FencingToken, Lease)} maps broker error code 47
 * {@code INVALID_PRODUCER_EPOCH} and broker error code 90 {@code PRODUCER_FENCED} onto
 * {@link FencedException}. That mapping is the whole mechanism by which a deposed leader
 * learns that it lost the role. No clock takes part in it.
 * {@link #register(Role, MemberId, long)} does not use the mapping. Its producer holds no
 * epoch for the role.
 *
 * <h2>All records of one role go to one partition</h2>
 *
 * <p>The succession rules rank candidates on the offset of their registration, so the
 * records of one role need a total order, and one partition gives that order. Both
 * producers pin the partition with {@link RolePartitioner}. See that class for why a
 * key-hashing partitioner breaks the order.
 *
 * <h2>The read is committed</h2>
 *
 * <p>{@link #readRoleRecords(Role)} drives a caller-owned consumer with
 * {@code assign}, {@code seek}, and {@code poll}, so the read needs no consumer group.
 * Configure that consumer with {@code isolation.level=read_committed}, so an aborted
 * lease write stays invisible. The read walks from the first offset of the partition to
 * the end offset, and it drops every record whose key belongs to another role.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * try (Admin admin = Admin.create(adminSettings);
 *         Producer<byte[], byte[]> registrar = new KafkaProducer<>(plainSettings);
 *         Consumer<byte[], byte[]> reader = new KafkaConsumer<>(committedReadSettings);
 *         KafkaCoordinationTransport transport = new KafkaCoordinationTransport(
 *             admin, registrar, role -> new KafkaProducer<>(transactionalSettings(role)),
 *             reader)) {
 *     transport.register(role, me, System.currentTimeMillis());
 * }
 * }</pre>
 */
public final class KafkaCoordinationTransport implements CoordinationTransport {
    /** The poll timeout a read uses when the caller names none. */
    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(1);

    private final Function<Role, Optional<FencingToken>> epochReader;
    private final Producer<byte[], byte[]> registrationProducer;
    private final Function<Role, Producer<byte[], byte[]>> transactionalProducers;
    private final Consumer<byte[], byte[]> stateConsumer;
    private final Duration pollTimeout;
    private final Map<Role, Producer<byte[], byte[]>> bound = new ConcurrentHashMap<>();
    private final Map<Role, FencingToken> tokens = new ConcurrentHashMap<>();
    private volatile int partitions;

    /**
     * Creates a transport with the default poll timeout.
     *
     * @param admin the client that answers {@code DescribeTransactions}
     * @param registrationProducer the plain producer that appends registration records
     * @param transactionalProducers builds the producer of one role, with
     *     {@code transactional.id} set to the role name
     * @param stateConsumer the consumer that reads {@link CoordinationCodec#TOPIC}
     * @throws NullPointerException if an argument is null
     */
    public KafkaCoordinationTransport(
            Admin admin,
            Producer<byte[], byte[]> registrationProducer,
            Function<Role, Producer<byte[], byte[]>> transactionalProducers,
            Consumer<byte[], byte[]> stateConsumer) {
        this(admin, registrationProducer, transactionalProducers, stateConsumer,
                DEFAULT_POLL_TIMEOUT);
    }

    /**
     * Creates a transport.
     *
     * @param admin the client that answers {@code DescribeTransactions}
     * @param registrationProducer the plain producer that appends registration records
     * @param transactionalProducers builds the producer of one role, with
     *     {@code transactional.id} set to the role name
     * @param stateConsumer the consumer that reads {@link CoordinationCodec#TOPIC}
     * @param pollTimeout how long one poll of the coordination topic may block
     * @throws NullPointerException if an argument is null
     */
    public KafkaCoordinationTransport(
            Admin admin,
            Producer<byte[], byte[]> registrationProducer,
            Function<Role, Producer<byte[], byte[]>> transactionalProducers,
            Consumer<byte[], byte[]> stateConsumer,
            Duration pollTimeout) {
        this(epochReaderOf(admin), registrationProducer, transactionalProducers, stateConsumer,
                pollTimeout);
    }

    /** Builds a transport over an epoch seam that a test replaces. */
    KafkaCoordinationTransport(
            Function<Role, Optional<FencingToken>> epochReader,
            Producer<byte[], byte[]> registrationProducer,
            Function<Role, Producer<byte[], byte[]>> transactionalProducers,
            Consumer<byte[], byte[]> stateConsumer,
            Duration pollTimeout) {
        this.epochReader = Objects.requireNonNull(epochReader, "epochReader");
        this.registrationProducer =
                Objects.requireNonNull(registrationProducer, "registrationProducer");
        this.transactionalProducers =
                Objects.requireNonNull(transactionalProducers, "transactionalProducers");
        this.stateConsumer = Objects.requireNonNull(stateConsumer, "stateConsumer");
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
    }

    /**
     * Mints a new epoch for a role and fences the member that held it.
     *
     * <p>A Kafka producer calls {@code initTransactions} once, and the epoch it receives
     * binds to that instance for its whole life. A second call for one role builds a
     * second producer and closes the first. The token of the first call stops
     * working at that point, which is correct: the coordinator superseded it.
     *
     * @param role the role to take
     * @return the token the transaction coordinator minted
     * @throws CoordinationException if {@code InitProducerId} fails, or if the
     *     coordinator reports no transaction for the role after the call
     */
    @Override
    public FencingToken acquireEpoch(Role role) {
        Objects.requireNonNull(role, "role");
        release(role);
        Producer<byte[], byte[]> producer = transactionalProducers.apply(role);
        try {
            producer.initTransactions();
        } catch (KafkaException error) {
            producer.close();
            throw new CoordinationException(
                    "InitProducerId failed for role " + role + ": " + error.getMessage(), error);
        }
        Optional<FencingToken> minted = describe(role);
        if (minted.isEmpty()) {
            producer.close();
            throw new CoordinationException(
                    "the transaction coordinator reports no transaction for role " + role
                            + " after InitProducerId");
        }
        bound.put(role, producer);
        tokens.put(role, minted.orElseThrow());
        return minted.orElseThrow();
    }

    /**
     * Reads the whole partition of a role and returns the records in offset order.
     *
     * @param role the role to read
     * @return the decoded records, oldest first
     * @throws CoordinationException if the topic is absent, or if a record of this role
     *     does not decode
     */
    @Override
    public List<CoordinationEntry> readRoleRecords(Role role) {
        TopicPartition partition = new TopicPartition(CoordinationCodec.TOPIC, partitionOf(role));
        List<TopicPartition> assignment = List.of(partition);
        stateConsumer.assign(assignment);
        stateConsumer.seekToBeginning(assignment);
        long end = stateConsumer.endOffsets(assignment).getOrDefault(partition, 0L);
        List<CoordinationEntry> entries = new ArrayList<>();
        while (stateConsumer.position(partition) < end) {
            var polled = stateConsumer.poll(pollTimeout);
            if (polled.isEmpty()) {
                break;
            }
            for (ConsumerRecord<byte[], byte[]> record : polled.records(partition)) {
                decode(record).filter(entry -> entry.key().role().equals(role))
                        .ifPresent(entries::add);
            }
        }
        return List.copyOf(entries);
    }

    /**
     * Appends the registration of one member to the partition of a role.
     *
     * @param role the role the member competes for
     * @param member the member that announces itself
     * @param registeredAtMillis the instant of the registration, in milliseconds since
     *     the Unix epoch
     * @throws CoordinationException if the append fails
     */
    @Override
    public void register(Role role, MemberId member, long registeredAtMillis) {
        Objects.requireNonNull(member, "member");
        byte[] key = CoordinationCodec.encodeKey(CoordinationKey.registration(role, member));
        byte[] value = CoordinationCodec.encodeValue(new Registration(member, registeredAtMillis));
        try {
            registrationProducer.send(new ProducerRecord<>(
                    CoordinationCodec.TOPIC, partitionOf(role), key, value)).get();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new CoordinationException(
                    "the registration of member " + member + " for role " + role
                            + " was interrupted", error);
        } catch (ExecutionException error) {
            throw new CoordinationException("the registration of member " + member + " for role "
                    + role + " failed: " + error.getCause().getMessage(), error.getCause());
        }
    }

    /**
     * Writes the lease of a role in a transaction under one token.
     *
     * @param role the role the lease belongs to
     * @param token the token this transport minted for the role
     * @param lease the lease record to write
     * @throws FencedException if the broker rejects the token
     * @throws CoordinationException if this transport never minted the token, or if the
     *     write fails for another reason
     */
    @Override
    public void writeLease(Role role, FencingToken token, Lease lease) {
        Objects.requireNonNull(lease, "lease");
        write(role, token, CoordinationCodec.encodeValue(lease));
    }

    /**
     * Clears the lease of a role with a tombstone, in a transaction under one token.
     *
     * @param role the role to release
     * @param token the token this transport minted for the role
     * @throws FencedException if the broker rejects the token
     * @throws CoordinationException if this transport never minted the token, or if the
     *     write fails for another reason
     */
    @Override
    public void clearLease(Role role, FencingToken token) {
        write(role, token, null);
    }

    /**
     * Asks the transaction coordinator which token holds a role now.
     *
     * @param role the role to look up
     * @return the current token, and empty when no member has ever taken the role
     * @throws CoordinationException if the coordinator lookup fails
     */
    @Override
    public Optional<FencingToken> describe(Role role) {
        Objects.requireNonNull(role, "role");
        return epochReader.apply(role);
    }

    /**
     * Closes the transactional producers this transport built.
     *
     * <p>The caller keeps the admin client, the registration producer, and the consumer,
     * so this method leaves those three open.
     */
    @Override
    public void close() {
        bound.values().forEach(Producer::close);
        bound.clear();
        tokens.clear();
    }

    /** Reads the token of a role from the transaction coordinator. */
    private static Function<Role, Optional<FencingToken>> epochReaderOf(Admin admin) {
        Objects.requireNonNull(admin, "admin");
        return role -> tokenOf(role,
                admin.describeTransactions(Set.of(role.name())).description(role.name()));
    }

    /**
     * Maps the answer of {@code DescribeTransactions} onto a fencing token.
     *
     * <p>A role that no member has taken reports a negative producer id or raises
     * {@code TransactionalIdNotFoundException}, and both answers are an empty result.
     *
     * @param role the role the description belongs to
     * @param description the pending description the coordinator returned
     * @return the token of the role, and empty when no member has ever taken it
     */
    static Optional<FencingToken> tokenOf(
            Role role, KafkaFuture<TransactionDescription> description) {
        TransactionDescription described;
        try {
            described = description.get();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new CoordinationException(
                    "the description of role " + role + " was interrupted", error);
        } catch (ExecutionException error) {
            if (error.getCause() instanceof TransactionalIdNotFoundException) {
                return Optional.empty();
            }
            throw new CoordinationException("the transaction coordinator did not describe role "
                    + role + ": " + error.getCause().getMessage(), error.getCause());
        }
        if (described.producerId() < 0 || described.producerEpoch() < 0) {
            return Optional.empty();
        }
        return Optional.of(
                FencingToken.of(described.producerId(), (short) described.producerEpoch()));
    }

    private void write(Role role, FencingToken token, byte[] value) {
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(token, "token");
        Producer<byte[], byte[]> producer = bound.get(role);
        if (producer == null || !token.equals(tokens.get(role))) {
            throw new CoordinationException(
                    "this transport did not mint token " + token + " for role " + role);
        }
        byte[] key = CoordinationCodec.encodeKey(CoordinationKey.lease(role));
        try {
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(
                    CoordinationCodec.TOPIC, partitionOf(role), key, value));
            producer.commitTransaction();
        } catch (ProducerFencedException | InvalidProducerEpochException fenced) {
            throw new FencedException(role, fenced);
        } catch (KafkaException error) {
            throw new CoordinationException("the lease write for role " + role + " failed: "
                    + error.getMessage(), error);
        }
    }

    private void release(Role role) {
        tokens.remove(role);
        Producer<byte[], byte[]> superseded = bound.remove(role);
        if (superseded != null) {
            superseded.close();
        }
    }

    private int partitionOf(Role role) {
        int count = partitions;
        if (count < 1) {
            List<PartitionInfo> info = stateConsumer.partitionsFor(CoordinationCodec.TOPIC);
            if (info == null || info.isEmpty()) {
                throw new CoordinationException(
                        "topic " + CoordinationCodec.TOPIC + " has no partitions");
            }
            count = info.size();
            partitions = count;
        }
        return RolePartitioner.partitionFor(role, count);
    }

    private Optional<CoordinationEntry> decode(ConsumerRecord<byte[], byte[]> record) {
        if (record.key() == null) {
            throw new CoordinationException("a record of " + CoordinationCodec.TOPIC
                    + " at offset " + record.offset() + " carries no key");
        }
        CoordinationKey key = CoordinationCodec.decodeKey(record.key());
        byte[] value = CoordinationCodec.isTombstone(record.value()) ? null : record.value();
        return Optional.of(new CoordinationEntry(
                record.offset(), key, CoordinationCodec.decodeValue(key.kind(), value)));
    }
}
