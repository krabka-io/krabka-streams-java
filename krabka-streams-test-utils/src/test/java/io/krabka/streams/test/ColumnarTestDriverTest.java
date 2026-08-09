package io.krabka.streams.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.krabka.streams.columnar.ColumnarTopology;
import io.krabka.streams.columnar.JsonRowBridge;
import io.krabka.streams.columnar.RowCodec;
import io.krabka.streams.columnar.ProduceRecord;
import io.krabka.streams.columnar.RecordHeader;
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

            var headers = List.of(new RecordHeader("trace-id", bytes("abc")));
            driver.pipeInput("in", 0, bytes("a"), bytes("first"), 10, headers);
            driver.pipeInput("in", 0, bytes("b"), bytes("second"), 11);

            assertThat(driver.outputSize("out")).isEqualTo(2);
            assertThat(driver.readOutput("out"))
                    .usingRecursiveComparison()
                    .isEqualTo(new ProduceRecord(bytes("a"), bytes("first"), 10, headers));
            assertThat(driver.drainOutput("out")).hasSize(1);
            assertThrows(NoSuchElementException.class, () -> driver.readOutput("out"));

            var fault = new IllegalStateException("injected");
            driver.failNext(fault);
            assertThat(assertThrows(
                            IllegalStateException.class,
                            () -> driver.pipeInput("in", 0, null, bytes("third"), 12)))
                    .isSameAs(fault);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
