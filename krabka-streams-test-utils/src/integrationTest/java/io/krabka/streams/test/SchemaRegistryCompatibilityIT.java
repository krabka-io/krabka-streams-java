package io.krabka.streams.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.krabka.streams.schema.AvroSerde;
import io.krabka.streams.schema.KrabkaSchemaRegistryClient;
import io.krabka.streams.schema.Role;
import io.krabka.streams.schema.SchemaCache;
import io.krabka.streams.schema.SchemaKind;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "KRABKA_INTEGRATION_SCHEMA_REGISTRY", matches = ".+")
class SchemaRegistryCompatibilityIT {
    @Test
    void registersLooksUpFetchesAndRoundTripsAvro() {
        var uri = URI.create(System.getenv("KRABKA_INTEGRATION_SCHEMA_REGISTRY"));
        var client = new KrabkaSchemaRegistryClient(uri);
        var cache = new SchemaCache(client);
        var schema = new Schema.Parser().parse("""
                {"type":"record","name":"Event","namespace":"io.krabka.test",
                 "fields":[{"name":"name","type":"string"}]}
                """);
        String topic = "registry-it-" + UUID.randomUUID().toString().replace("-", "");
        String subject = topic + "-value";
        var serde = AvroSerde.generic(schema, cache, Role.VALUE);
        String registeredSchema = schema.toString();
        serde.registerSubject(topic);
        cache.prewarm().join();

        int id = cache.idForSubject(subject).orElseThrow();
        var value = new GenericData.Record(schema);
        value.put("name", "krabka");
        byte[] encoded = serde.serializer().serialize(topic, value);
        var decoded = serde.deserializer().deserialize(topic, encoded);

        assertEquals("krabka", decoded.get("name").toString());
        assertEquals(id, client.lookup(subject, SchemaKind.AVRO, registeredSchema, null).join());
        assertEquals(id, client.latestId(subject).join());
        assertEquals(registeredSchema, client.schemaById(id).join().schema());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "KRABKA_INTEGRATION_BOOTSTRAP", matches = ".+")
    void evolvesRecordsThroughBrokerWithWarmAndColdCaches() throws Exception {
        String bootstrap = System.getenv("KRABKA_INTEGRATION_BOOTSTRAP");
        URI registry = URI.create(System.getenv("KRABKA_INTEGRATION_SCHEMA_REGISTRY"));
        String topic = System.getenv().getOrDefault(
                "KRABKA_INTEGRATION_SCHEMA_TOPIC",
                "schema-evolution-" + UUID.randomUUID().toString().replace("-", ""));
        String phase = System.getenv().getOrDefault("KRABKA_INTEGRATION_SCHEMA_PHASE", "roundtrip");
        var oldSchema = new Schema.Parser().parse("""
                {"type":"record","name":"Event","namespace":"io.krabka.test",
                 "fields":[{"name":"name","type":"string"}]}
                """);
        var newSchema = new Schema.Parser().parse("""
                {"type":"record","name":"Event","namespace":"io.krabka.test",
                 "fields":[{"name":"name","type":"string"},
                           {"name":"source","type":"string","default":"legacy"}]}
                """);

        if (!phase.equals("read")) {
            createValidatedTopic(bootstrap, topic);
            produce(bootstrap, topic, registry, oldSchema, "old", null);
            produce(bootstrap, topic, registry, newSchema, "new", "v2");
            var incompatible = new Schema.Parser().parse("""
                    {"type":"record","name":"Event","namespace":"io.krabka.test",
                     "fields":[{"name":"name","type":"int"}]}
                    """);
            assertThrows(
                    java.util.concurrent.CompletionException.class,
                    () -> new KrabkaSchemaRegistryClient(registry)
                            .register(topic + "-value", SchemaKind.AVRO, incompatible.toString(), null)
                            .join());
        }
        if (!phase.equals("write")) {
            assertEquals(
                    java.util.List.of("old:legacy", "new:v2"),
                    consume(bootstrap, topic, registry, newSchema));
        }
    }

    private static void createValidatedTopic(String bootstrap, String topic) throws Exception {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(Set.of(new NewTopic(topic, 1, (short) 1)
                            .configs(Map.of("schema.validation.value", "true", TopicConfig.CLEANUP_POLICY_CONFIG, "compact"))))
                    .all()
                    .get();
        }
    }

    private static void produce(
            String bootstrap, String topic, URI registry, Schema schema, String name, String source)
            throws Exception {
        var cache = new SchemaCache(new KrabkaSchemaRegistryClient(registry));
        var serde = AvroSerde.generic(schema, cache, Role.VALUE);
        serde.registerSubject(topic);
        cache.prewarm().join();
        var value = new GenericData.Record(schema);
        value.put("name", name);
        if (source != null) {
            value.put("source", source);
        }
        try (var producer = new KafkaProducer<byte[], byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG,
                "all"))) {
            producer.send(new ProducerRecord<>(topic, name.getBytes(), serde.serializer().serialize(topic, value)))
                    .get();
        }
    }

    private static java.util.List<String> consume(String bootstrap, String topic, URI registry, Schema schema) {
        var cache = new SchemaCache(new KrabkaSchemaRegistryClient(registry));
        var serde = AvroSerde.generic(schema, cache, Role.VALUE);
        var result = new ArrayList<String>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG,
                "schema-evolution-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class))) {
            consumer.subscribe(Set.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (result.size() < 2 && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    GenericRecord decoded;
                    for (; ; ) {
                        try {
                            decoded = serde.deserializer().deserialize(topic, record.value());
                            break;
                        } catch (io.krabka.streams.schema.SchemaFetchPendingException pending) {
                            Thread.onSpinWait();
                        }
                    }
                    result.add(decoded.get("name") + ":" + decoded.get("source"));
                    assertEquals(decoded, serde.deserializer().deserialize(topic, record.value()));
                }
            }
        }
        return result;
    }
}
