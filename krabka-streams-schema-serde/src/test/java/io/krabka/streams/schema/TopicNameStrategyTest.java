package io.krabka.streams.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TopicNameStrategyTest {
    @Test
    void usesKeyAndValueSuffixes() {
        var strategy = new TopicNameStrategy();

        assertEquals("orders-key", strategy.subject("orders", Role.KEY));
        assertEquals("orders-value", strategy.subject("orders", Role.VALUE));
    }
}
