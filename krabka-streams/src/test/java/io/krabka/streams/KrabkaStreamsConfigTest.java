package io.krabka.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;
import org.junit.jupiter.api.Test;

class KrabkaStreamsConfigTest {
    @Test
    void addsStreamsGroupProtocol() {
        var source = Map.of("application.id", "example");

        var result = KrabkaStreamsConfig.withDefaults(source);

        assertEquals("streams", result.get("group.protocol"));
        assertEquals("example", result.get("application.id"));
        assertFalse(source.containsKey("group.protocol"));
    }

    @Test
    void keepsExplicitGroupProtocol() {
        var result = KrabkaStreamsConfig.withDefaults(Map.of("group.protocol", "classic"));

        assertEquals("classic", result.get("group.protocol"));
    }
}
