package io.krabka.streams.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

class KafkaStreamsParityTest {
    private static final Serdes.StringSerde STRINGS = new Serdes.StringSerde();

    @Test
    void runsDslAggregationAndStateStore() {
        var builder = new StreamsBuilder();
        builder.stream("input", Consumed.with(STRINGS, STRINGS))
                .filter((key, value) -> value.startsWith("keep"))
                .mapValues(value -> value.toUpperCase())
                .groupByKey()
                .count(Materialized.as("counts"))
                .toStream()
                .to("output", Produced.with(STRINGS, Serdes.Long()));

        try (var driver = new TopologyTestDriver(builder.build(), properties("dsl"))) {
            var input = driver.createInputTopic("input", STRINGS.serializer(), STRINGS.serializer());
            var output = driver.createOutputTopic("output", STRINGS.deserializer(), Serdes.Long().deserializer());

            input.pipeInput("a", "drop");
            input.pipeInput("a", "keep-one");
            input.pipeInput("a", "keep-two");

            assertEquals(Map.of("a", 2L), output.readKeyValuesToMap());
            assertEquals(2L, driver.<String, Long>getKeyValueStore("counts").get("a"));
        }
    }

    @Test
    void runsStreamJoinAndSuppressedWindow() {
        var builder = new StreamsBuilder();
        var left = builder.stream("left", Consumed.with(STRINGS, STRINGS));
        var right = builder.stream("right", Consumed.with(STRINGS, STRINGS));
        left.join(
                        right,
                        (first, second) -> first + "+" + second,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)))
                .to("joined", Produced.with(STRINGS, STRINGS));
        left.groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .selectKey((window, count) -> window.key())
                .to("windowed", Produced.with(STRINGS, Serdes.Long()));

        try (var driver = new TopologyTestDriver(builder.build(), properties("joins"))) {
            var leftInput = driver.createInputTopic(
                    "left", STRINGS.serializer(), STRINGS.serializer(), Instant.EPOCH, Duration.ZERO);
            var rightInput = driver.createInputTopic(
                    "right", STRINGS.serializer(), STRINGS.serializer(), Instant.EPOCH, Duration.ZERO);
            var joined = driver.createOutputTopic("joined", STRINGS.deserializer(), STRINGS.deserializer());
            var windowed = driver.createOutputTopic("windowed", STRINGS.deserializer(), Serdes.Long().deserializer());

            leftInput.pipeInput("a", "L", 0L);
            rightInput.pipeInput("a", "R", 1_000L);
            leftInput.pipeInput("advance", "L2", 11_000L);

            assertEquals(KeyValue.pair("a", "L+R"), joined.readKeyValue());
            assertEquals(KeyValue.pair("a", 1L), windowed.readKeyValue());
        }
    }

    @Test
    void runsGlobalTableAndVersionedStore() {
        var builder = new StreamsBuilder();
        GlobalKTable<String, String> lookup = builder.globalTable(
                "lookup", Consumed.with(STRINGS, STRINGS), Materialized.as("global-lookup"));
        builder.stream("orders", Consumed.with(STRINGS, STRINGS))
                .join(lookup, (orderId, sku) -> sku, (sku, description) -> sku + ":" + description)
                .to("enriched", Produced.with(STRINGS, STRINGS));

        var versionedSupplier = Stores.persistentVersionedKeyValueStore("versions", Duration.ofMinutes(5));
        builder.table(
                "versions-input",
                Consumed.with(STRINGS, STRINGS),
                Materialized.<String, String>as(versionedSupplier));

        try (var driver = new TopologyTestDriver(builder.build(), properties("tables"))) {
            var lookupInput = driver.createInputTopic("lookup", STRINGS.serializer(), STRINGS.serializer());
            var orderInput = driver.createInputTopic("orders", STRINGS.serializer(), STRINGS.serializer());
            var versionInput = driver.createInputTopic(
                    "versions-input", STRINGS.serializer(), STRINGS.serializer(), Instant.EPOCH, Duration.ZERO);
            var enriched = driver.createOutputTopic("enriched", STRINGS.deserializer(), STRINGS.deserializer());

            lookupInput.pipeInput("sku-1", "widget");
            orderInput.pipeInput("order-1", "sku-1");
            versionInput.pipeInput("a", "old", 100L);
            versionInput.pipeInput("a", "new", 200L);

            assertEquals(KeyValue.pair("order-1", "sku-1:widget"), enriched.readKeyValue());
            var versions = driver.<String, String>getVersionedKeyValueStore("versions");
            assertEquals("new", versions.get("a").value());
            assertEquals("old", versions.get("a", 150L).value());

            var query = StateQueryRequest.inStore("versions")
                    .withQuery(KeyQuery.<String, String>withKey("a"))
                    .withAllPartitions();
            assertEquals("versions", query.getStoreName());
            assertTrue(query.isAllPartitions());
        }
    }

    @Test
    void runsProcessorApiAndPunctuator() {
        var builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("processor-counts"), STRINGS, Serdes.Integer()));
        builder.stream("processor-input", Consumed.with(STRINGS, STRINGS))
                .<String, String>process(CountingProcessor::new, "processor-counts")
                .to("processor-output", Produced.with(STRINGS, STRINGS));

        try (var driver = new TopologyTestDriver(builder.build(), properties("processor"))) {
            var input = driver.createInputTopic(
                    "processor-input", STRINGS.serializer(), STRINGS.serializer());
            var output = driver.createOutputTopic(
                    "processor-output", STRINGS.deserializer(), STRINGS.deserializer());

            input.pipeInput("a", "value");
            driver.advanceWallClockTime(Duration.ofSeconds(1));

            assertEquals(KeyValue.pair("a", "VALUE"), output.readKeyValue());
            assertEquals(KeyValue.pair("punctuator", "tick"), output.readKeyValue());
            assertTrue(output.isEmpty());
            assertEquals(1, driver.<String, Integer>getKeyValueStore("processor-counts").get("a"));
        }
    }

    private static Properties properties(String suffix) {
        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "krabka-parity-" + suffix);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return properties;
    }

    private static final class CountingProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;
        private KeyValueStore<String, Integer> counts;

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
            counts = context.getStateStore("processor-counts");
            context.schedule(
                    Duration.ofMillis(100),
                    PunctuationType.WALL_CLOCK_TIME,
                    timestamp -> context.forward(new Record<>("punctuator", "tick", timestamp)));
        }

        @Override
        public void process(Record<String, String> record) {
            Integer previous = counts.get(record.key());
            counts.put(record.key(), previous == null ? 1 : previous + 1);
            context.forward(record.withValue(record.value().toUpperCase()));
        }
    }
}
