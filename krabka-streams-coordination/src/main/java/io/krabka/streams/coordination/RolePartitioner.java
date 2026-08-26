package io.krabka.streams.coordination;

import java.util.Objects;
import org.apache.kafka.common.utils.Utils;

/**
 * Computes the partition of {@link CoordinationCodec#TOPIC} that holds every record of
 * one role.
 *
 * <p>The rule is Kafka's own key partitioning: {@code murmur2} of the role name in
 * UTF-8, masked with {@code Utils.toPositive}, and then the remainder of the partition
 * count. The mask clears the sign bit. It is not an absolute value, and the two differ
 * for every negative hash. {@code krabka-client-rs} and {@code krabka-streams-go} write
 * the same topic, so they compute the same rule. A change here needs the same change in
 * the two ports.
 *
 * <p>Both writers of a role pin the partition with this rule. The pin is a correctness
 * requirement and not a preference. Kafka's default partitioner hashes the record key.
 * The registration key of a role and the lease key of the same role differ, because the
 * registration key names a member and the lease key does not. A partitioner that reads
 * the key puts the two kinds in two partitions, and the total order that the succession
 * rules rank on is gone.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * int partition = RolePartitioner.partitionFor(Role.of("controller"), 16);
 * producer.send(new ProducerRecord<>(CoordinationCodec.TOPIC, partition, key, value));
 * }</pre>
 */
public final class RolePartitioner {
    /**
     * The partition count this module assumes for {@link CoordinationCodec#TOPIC}.
     *
     * <p>Read the real count of the cluster with {@code partitionsFor} when an operator
     * created the topic with another count. A wrong count sends the records of a role to
     * the wrong partition, and a reader then finds no state.
     */
    public static final int DEFAULT_PARTITIONS = 16;

    private RolePartitioner() {
    }

    /**
     * Returns the partition that holds every record of a role.
     *
     * @param role the role to place
     * @param partitions the partition count of {@link CoordinationCodec#TOPIC}
     * @return the partition number, from zero up to one below the partition count
     * @throws CoordinationException if the partition count is below one
     * @throws NullPointerException if the role is null
     */
    public static int partitionFor(Role role, int partitions) {
        Objects.requireNonNull(role, "role");
        if (partitions < 1) {
            throw new CoordinationException("topic " + CoordinationCodec.TOPIC
                    + " needs at least one partition, got " + partitions);
        }
        return Utils.toPositive(Utils.murmur2(role.bytes())) % partitions;
    }
}
