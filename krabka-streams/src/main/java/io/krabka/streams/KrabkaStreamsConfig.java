package io.krabka.streams;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/** Applies the client settings that a krabka streams group needs. */
public final class KrabkaStreamsConfig {
    public static final String GROUP_PROTOCOL_CONFIG = "group.protocol";
    public static final String STREAMS_GROUP_PROTOCOL = "streams";

    private KrabkaStreamsConfig() {
    }

    /**
     * Copies the supplied settings and adds krabka defaults.
     * Explicit settings keep their values.
     *
     * @param settings source settings
     * @return an independent properties object
     */
    public static Properties withDefaults(Map<?, ?> settings) {
        Objects.requireNonNull(settings, "settings");
        var result = new Properties();
        settings.forEach(result::put);
        result.putIfAbsent(GROUP_PROTOCOL_CONFIG, STREAMS_GROUP_PROTOCOL);
        return result;
    }
}
