package io.krabka.streams;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Applies the client settings that a krabka streams group needs.
 *
 * <p>krabka brokers coordinate Kafka Streams applications through the streams group
 * protocol. The only krabka-specific configuration step is to enable that protocol,
 * which {@link #withDefaults(Map)} does for you while leaving every setting you supply
 * untouched.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var settings = Map.<String, Object>of(
 *     StreamsConfig.APPLICATION_ID_CONFIG, "order-counter",
 *     StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 *
 * var streams = new KafkaStreams(topology, KrabkaStreamsConfig.withDefaults(settings));
 * streams.start();
 * }</pre>
 *
 * <p>To opt out of a default, set the key explicitly; explicit settings always win:
 *
 * <pre>{@code
 * var settings = Map.<String, Object>of(
 *     StreamsConfig.APPLICATION_ID_CONFIG, "order-counter",
 *     StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
 *     KrabkaStreamsConfig.GROUP_PROTOCOL_CONFIG, "classic");
 * }</pre>
 */
public final class KrabkaStreamsConfig {
    /** The Kafka client configuration key that selects the group protocol. */
    public static final String GROUP_PROTOCOL_CONFIG = "group.protocol";

    /** The {@code group.protocol} value that enables the streams group protocol. */
    public static final String STREAMS_GROUP_PROTOCOL = "streams";

    private KrabkaStreamsConfig() {
    }

    /**
     * Copies the supplied settings and adds krabka defaults.
     * Explicit settings keep their values.
     *
     * <p>Currently the only default is {@link #GROUP_PROTOCOL_CONFIG} set to
     * {@link #STREAMS_GROUP_PROTOCOL}. The supplied map is not modified.
     *
     * @param settings source settings, typically keyed by {@code StreamsConfig} constants
     * @return an independent properties object
     * @throws NullPointerException if {@code settings} is null
     */
    public static Properties withDefaults(Map<?, ?> settings) {
        Objects.requireNonNull(settings, "settings");
        var result = new Properties();
        settings.forEach(result::put);
        result.putIfAbsent(GROUP_PROTOCOL_CONFIG, STREAMS_GROUP_PROTOCOL);
        return result;
    }
}
