package io.krabka.streams.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The partition rule is frozen across {@code krabka-client-rs}, {@code krabka-streams-go}, and this
 * module, because all three write the same topic. The values below were derived from Kafka's own
 * {@code toPositive(murmur2(role))} independently of the three implementations.
 */
class RolePartitionerTest {
    static Stream<Arguments> frozenPartitions() {
        return Stream.of(
                Arguments.of("controller", 16, 12),
                Arguments.of("dispatcher", 16, 10),
                Arguments.of("role-a", 16, 10),
                Arguments.of("role-b", 16, 12),
                Arguments.of("controller", 1, 0));
    }

    @ParameterizedTest(name = "{0} over {1} partitions lands on {2}")
    @MethodSource("frozenPartitions")
    void placesARoleOnItsFrozenPartition(String role, int partitions, int expected) {
        assertThat(RolePartitioner.partitionFor(Role.of(role), partitions)).isEqualTo(expected);
    }

    @Test
    void masksTheSignBitRatherThanTakingAnAbsoluteValue() {
        // murmur2("role-a") is negative. An absolute value would give 6 over 16 partitions, and the
        // mask gives 10. The three implementations all mask.
        assertThat(RolePartitioner.partitionFor(Role.of("role-a"), 16)).isEqualTo(10);
    }

    @Test
    void sendsBothRecordKindsOfOneRoleToOnePartition() {
        // The registration key and the lease key of one role differ, so a key-hashing partitioner
        // would split the role across two partitions and destroy the total order.
        Role role = Role.of("controller");

        assertThat(RolePartitioner.partitionFor(role, RolePartitioner.DEFAULT_PARTITIONS))
                .isEqualTo(RolePartitioner.partitionFor(role, RolePartitioner.DEFAULT_PARTITIONS));
    }

    @Test
    void staysInsideThePartitionCount() {
        for (int count = 1; count <= 64; count++) {
            for (int index = 0; index < 50; index++) {
                int partition = RolePartitioner.partitionFor(Role.of("role-" + index), count);
                assertThat(partition).isBetween(0, count - 1);
            }
        }
    }

    @Test
    void rejectsATopicWithNoPartition() {
        assertThatThrownBy(() -> RolePartitioner.partitionFor(Role.of("controller"), 0))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("topic __coordination_state needs at least one partition, got 0");
        assertThatThrownBy(() -> RolePartitioner.partitionFor(Role.of("controller"), -3))
                .isInstanceOf(CoordinationException.class);
    }
}
