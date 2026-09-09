package io.krabka.streams.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.krabka.streams.coordination.CoordinationClient;
import io.krabka.streams.coordination.CoordinationCodec;
import io.krabka.streams.coordination.FencedException;
import io.krabka.streams.coordination.KafkaCoordinationTransport;
import io.krabka.streams.coordination.LeaseConfig;
import io.krabka.streams.coordination.MemberId;
import io.krabka.streams.coordination.Role;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "KRABKA_INTEGRATION_BOOTSTRAP", matches = ".+")
class CoordinationCompatibilityIT {
    private static final Duration LEASE = Duration.ofMillis(750);

    @Test
    void fencesTheOldLeaderAndAppendsARecoveredMemberToTheTail() throws Exception {
        String bootstrap = System.getenv("KRABKA_INTEGRATION_BOOTSTRAP");
        createTopic(bootstrap);
        Role role = Role.of("m19-" + UUID.randomUUID().toString().replace("-", ""));
        MemberId firstMember = MemberId.of("java-first");
        MemberId secondMember = MemberId.of("java-second");
        LeaseConfig leases = LeaseConfig.of(LEASE, Duration.ofMillis(200), Duration.ofMillis(100));

        try (TransportResources first = new TransportResources(bootstrap);
                TransportResources second = new TransportResources(bootstrap);
                CoordinationClient firstClient = first.client(leases);
                CoordinationClient secondClient = second.client(leases)) {
            var oldLeader = firstClient.acquire(role, firstMember, Duration.ofSeconds(10));
            Thread.sleep(LEASE.plusMillis(100).toMillis());
            var newLeader = secondClient.acquire(role, secondMember, Duration.ofSeconds(10));

            assertThat(newLeader.token().compareTo(oldLeader.token())).isPositive();
            assertThatThrownBy(oldLeader::renew).isInstanceOf(FencedException.class);

            second.transport.register(role, firstMember, System.currentTimeMillis());
            assertThat(secondClient.readState(role).roster())
                    .extracting(entry -> entry.member().id())
                    .containsExactly(secondMember.id(), firstMember.id());
            newLeader.close();
        }
    }

    private static void createTopic(String bootstrap) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            try {
                admin.createTopics(Set.of(new NewTopic(CoordinationCodec.TOPIC, 16, (short) 1)
                                .configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT))))
                        .all()
                        .get();
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof TopicExistsException)) {
                    throw error;
                }
            }
        }
    }

    private static final class TransportResources implements AutoCloseable {
        private final Admin admin;
        private final KafkaProducer<byte[], byte[]> registrar;
        private final KafkaConsumer<byte[], byte[]> reader;
        private final String bootstrap;
        private final KafkaCoordinationTransport transport;

        TransportResources(String bootstrap) {
            this.bootstrap = bootstrap;
            admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
            registrar = new KafkaProducer<>(producerSettings(bootstrap, null));
            reader = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ConsumerConfig.GROUP_ID_CONFIG, "m19-java-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class));
            transport = new KafkaCoordinationTransport(
                    admin,
                    registrar,
                    role -> new KafkaProducer<>(producerSettings(this.bootstrap, role.name())),
                    reader);
        }

        CoordinationClient client(LeaseConfig leases) {
            return new CoordinationClient(
                    transport, leases, System::currentTimeMillis, Duration.ofMillis(100));
        }

        private static Map<String, Object> producerSettings(String bootstrap, String transactionalId) {
            var settings = new java.util.HashMap<String, Object>();
            settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            settings.put(ProducerConfig.ACKS_CONFIG, "all");
            if (transactionalId != null) {
                settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
                settings.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10_000);
            }
            return Map.copyOf(settings);
        }

        @Override
        public void close() {
            transport.close();
            reader.close();
            registrar.close();
            admin.close();
        }
    }
}
