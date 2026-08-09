package io.krabka.streams.columnar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

class RowCodecTest {
    @Test
    void assemblesAndExplodesRows() {
        try (var allocator = new RootAllocator()) {
            var codec = new RowCodec<>(Serdes.String(), new JsonRowBridge<>(String.class), allocator);
            var records = List.of(
                    new ConsumedRecord(new byte[] {1}, "a".getBytes(StandardCharsets.UTF_8), 10, 0, 5),
                    new ConsumedRecord(new byte[] {2}, "b".getBytes(StandardCharsets.UTF_8), 11, 0, 6));

            try (var batch = codec.decode(records)) {
                assertEquals(2, batch.getRowCount());
                assertThat(batch.getSchema().getFields().stream().map(field -> field.getName()).toList())
                        .usingRecursiveComparison()
                        .isEqualTo(List.of(
                                "value", "__key", "__timestamp", "__partition", "__offset", "__headers"));

                var output = codec.encode(batch);
                assertThat(output)
                        .usingRecursiveComparison()
                        .isEqualTo(List.of(
                                new ProduceRecord(new byte[] {1}, "a".getBytes(StandardCharsets.UTF_8), 10),
                                new ProduceRecord(new byte[] {2}, "b".getBytes(StandardCharsets.UTF_8), 11)));
            }
        }
    }

    @Test
    void passesHeadersToKafkaSerdes() {
        var deserializedHeaders = new java.util.concurrent.atomic.AtomicReference<List<String>>();
        var serializedHeaders = new java.util.concurrent.atomic.AtomicReference<List<String>>();
        var serializer = new org.apache.kafka.common.serialization.Serializer<String>() {
            @Override
            public byte[] serialize(String topic, String value) {
                return value.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public byte[] serialize(
                    String topic, org.apache.kafka.common.header.Headers headers, String value) {
                serializedHeaders.set(keys(headers));
                return serialize(topic, value);
            }
        };
        var deserializer = new org.apache.kafka.common.serialization.Deserializer<String>() {
            @Override
            public String deserialize(String topic, byte[] value) {
                return new String(value, StandardCharsets.UTF_8);
            }

            @Override
            public String deserialize(
                    String topic, org.apache.kafka.common.header.Headers headers, byte[] value) {
                deserializedHeaders.set(keys(headers));
                return deserialize(topic, value);
            }
        };
        try (var allocator = new RootAllocator()) {
            var codec = new RowCodec<>(
                    Serdes.serdeFrom(serializer, deserializer),
                    new JsonRowBridge<>(String.class),
                    allocator);
            var headers = List.of(new RecordHeader("trace-id", new byte[] {1}));
            try (var batch = codec.decode(List.of(
                    new ConsumedRecord(null, "a".getBytes(StandardCharsets.UTF_8), 1, 0, 0, headers)))) {
                codec.encode(batch);
            }
        }

        assertThat(deserializedHeaders.get())
                .usingRecursiveComparison()
                .isEqualTo(List.of("trace-id"));
        assertThat(serializedHeaders.get())
                .usingRecursiveComparison()
                .isEqualTo(List.of("trace-id"));
    }

    private static List<String> keys(org.apache.kafka.common.header.Headers headers) {
        return java.util.stream.StreamSupport.stream(headers.spliterator(), false)
                .map(org.apache.kafka.common.header.Header::key)
                .toList();
    }
}
