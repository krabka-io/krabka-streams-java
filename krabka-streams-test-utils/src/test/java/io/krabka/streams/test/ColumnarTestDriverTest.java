package io.krabka.streams.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.krabka.streams.columnar.ColumnarTopology;
import io.krabka.streams.columnar.JsonRowBridge;
import io.krabka.streams.columnar.RowCodec;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

class ColumnarTestDriverTest {
    @Test
    void pipesInputAndQueuesOutput() {
        try (var allocator = new RootAllocator()) {
            var codec = new RowCodec<>(Serdes.String(), new JsonRowBridge<>(String.class), allocator);
            var topology = new ColumnarTopology(allocator);
            var source = topology.addSource("source", List.of("in"), codec);
            topology.addSink("sink", "out", codec, source);
            var driver = new ColumnarTestDriver(topology.build());

            driver.pipeInput("in", 0, bytes("a"), bytes("first"), 10);
            driver.pipeInput("in", 0, bytes("b"), bytes("second"), 11);

            assertEquals(2, driver.outputSize("out"));
            var first = driver.readOutput("out");
            assertArrayEquals(bytes("a"), first.key());
            assertArrayEquals(bytes("first"), first.value());
            assertEquals(10, first.timestamp());
            assertEquals(1, driver.drainOutput("out").size());
            assertThrows(NoSuchElementException.class, () -> driver.readOutput("out"));
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
