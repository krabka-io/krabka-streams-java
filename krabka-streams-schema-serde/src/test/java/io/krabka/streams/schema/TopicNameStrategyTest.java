package io.krabka.streams.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.testing.junit.testparameterinjector.junit5.TestParameterInjectorTest;
import com.google.testing.junit.testparameterinjector.junit5.TestParameters;

class TopicNameStrategyTest {
    @TestParameterInjectorTest
    @TestParameters("{role: KEY, expected: orders-key}")
    @TestParameters("{role: VALUE, expected: orders-value}")
    void usesRoleSuffix(Role role, String expected) {
        var strategy = new TopicNameStrategy();

        assertThat(strategy.subject("orders", role)).isEqualTo(expected);
    }
}
