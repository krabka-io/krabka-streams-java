/**
 * Defaults and entry points for Apache Kafka Streams applications.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var settings = Map.<String, Object>of(
 *     StreamsConfig.APPLICATION_ID_CONFIG, "orders",
 *     StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 * var streams = new KafkaStreams(topology, KrabkaStreamsConfig.withDefaults(settings));
 * streams.start();
 * }</pre>
 */
package io.krabka.streams;
