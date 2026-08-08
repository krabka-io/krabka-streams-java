package io.krabka.streams.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.krabka.streams.KrabkaStreamsConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "KRABKA_INTEGRATION_BOOTSTRAP", matches = ".+")
class BrokerCompatibilityIT {
    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final String STORE = "counts";

    @Test
    void runsStreamsProtocolEosStandbyIqv2AndRestore() throws Exception {
        String bootstrap = System.getenv("KRABKA_INTEGRATION_BOOTSTRAP");
        String runId = UUID.randomUUID().toString().replace("-", "");
        String applicationId = "krabka-java-it-" + runId;
        String inputTopic = applicationId + "-input";
        String outputTopic = applicationId + "-output";
        Path firstState = Files.createTempDirectory("krabka-streams-first-");
        Path secondState = Files.createTempDirectory("krabka-streams-second-");
        Path restoredState = Files.createTempDirectory("krabka-streams-restored-");

        createTopics(bootstrap, inputTopic, outputTopic);
        var first = new KafkaStreams(
                topology(inputTopic, outputTopic),
                streamsProperties(bootstrap, applicationId, firstState, "localhost:18081", 1));
        var second = new KafkaStreams(
                topology(inputTopic, outputTopic),
                streamsProperties(bootstrap, applicationId, secondState, "localhost:18082", 1));
        try {
            first.start();
            second.start();
            waitFor("both streams clients to run", () -> isRunning(first) && isRunning(second));

            var partitions = produceInput(bootstrap, inputTopic);
            assertEquals(Map.of("alpha", 1L, "beta", 1L), readOutput(bootstrap, outputTopic));
            assertEquals(1L, query(first, second, "alpha", partitions.get("alpha")));
            assertEquals(1L, query(first, second, "beta", partitions.get("beta")));
            waitFor("a standby task", () -> hasStandbyTask(first) || hasStandbyTask(second));
        } finally {
            first.close(TIMEOUT);
            second.close(TIMEOUT);
        }

        var restored = new KafkaStreams(
                topology(inputTopic, outputTopic),
                streamsProperties(bootstrap, applicationId, restoredState, "localhost:18083", 0));
        try {
            restored.start();
            waitFor("the restored streams client to run", () -> isRunning(restored));
            assertEquals(1L, query(restored, restored, "alpha", 0));
            assertEquals(1L, query(restored, restored, "beta", 1));
        } finally {
            restored.close(TIMEOUT);
            first.cleanUp();
            second.cleanUp();
            restored.cleanUp();
        }
    }

    private static Topology topology(String inputTopic, String outputTopic) {
        var builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE), Serdes.String(), Serdes.Long())
                .withLoggingEnabled(Map.of()));
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .<String, Long>process(CountingProcessor::new, STORE)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    private static Properties streamsProperties(
            String bootstrap,
            String applicationId,
            Path stateDirectory,
            String applicationServer,
            int standbyReplicas) {
        var settings = new HashMap<String, Object>();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        settings.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        settings.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        settings.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, standbyReplicas);
        settings.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString());
        settings.put(StreamsConfig.APPLICATION_SERVER_CONFIG, applicationServer);
        settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return KrabkaStreamsConfig.withDefaults(settings);
    }

    private static void createTopics(String bootstrap, String inputTopic, String outputTopic)
            throws InterruptedException, ExecutionException {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(Set.of(
                            new NewTopic(inputTopic, 2, (short) 1),
                            new NewTopic(outputTopic, 2, (short) 1)))
                    .all()
                    .get();
        }
    }

    private static Map<String, Integer> produceInput(String bootstrap, String topic) throws Exception {
        var settings = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all");
        try (var producer = new KafkaProducer<String, String>(settings)) {
            var partitions = new HashMap<String, Integer>();
            partitions.put("alpha", producer.send(new ProducerRecord<>(topic, 0, "alpha", "one")).get().partition());
            partitions.put("beta", producer.send(new ProducerRecord<>(topic, 1, "beta", "two")).get().partition());
            producer.flush();
            return Map.copyOf(partitions);
        }
    }

    private static Map<String, Long> readOutput(String bootstrap, String topic) {
        var settings = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, "krabka-java-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        try (var consumer = new KafkaConsumer<String, Long>(settings)) {
            consumer.subscribe(Set.of(topic));
            var values = new HashMap<String, Long>();
            waitFor("two committed output records", () -> {
                consumer.poll(Duration.ofMillis(250)).forEach(record -> values.put(record.key(), record.value()));
                return values.keySet().containsAll(Set.of("alpha", "beta"));
            });
            return Map.copyOf(values);
        }
    }

    private static long query(
            KafkaStreams first,
            KafkaStreams second,
            String key,
            int partition) {
        var request = StateQueryRequest.inStore(STORE)
                .withQuery(KeyQuery.<String, Long>withKey(key))
                .withPartitions(Set.of(partition))
                .requireActive();
        var answer = new long[] {-1L};
        waitFor("IQv2 result for " + key, () -> {
            for (var streams : List.of(first, second)) {
                var result = streams.query(request).getPartitionResults().get(partition);
                if (result != null && result.isSuccess()) {
                    answer[0] = result.getResult();
                    return true;
                }
            }
            return false;
        });
        return answer[0];
    }

    private static boolean isRunning(KafkaStreams streams) {
        return streams.state() == KafkaStreams.State.RUNNING;
    }

    private static boolean hasStandbyTask(KafkaStreams streams) {
        return streams.metadataForLocalThreads().stream()
                .anyMatch(metadata -> !metadata.standbyTasks().isEmpty());
    }

    private static void waitFor(String description, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        RuntimeException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (RuntimeException error) {
                lastFailure = error;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for " + description, error);
            }
        }
        var failure = new AssertionError("timed out while waiting for " + description);
        if (lastFailure != null) {
            failure.initCause(lastFailure);
        }
        throw failure;
    }

    private static final class CountingProcessor implements Processor<String, String, String, Long> {
        private ProcessorContext<String, Long> context;
        private KeyValueStore<String, Long> counts;

        @Override
        public void init(ProcessorContext<String, Long> context) {
            this.context = context;
            counts = context.getStateStore(STORE);
        }

        @Override
        public void process(Record<String, String> record) {
            Long current = counts.get(record.key());
            long next = current == null ? 1L : current + 1L;
            counts.put(record.key(), next);
            context.forward(new Record<>(record.key(), next, record.timestamp(), record.headers()));
        }
    }
}
