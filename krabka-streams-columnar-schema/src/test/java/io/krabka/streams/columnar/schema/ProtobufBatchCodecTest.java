package io.krabka.streams.columnar.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.DynamicMessage;
import io.krabka.streams.columnar.ConsumedRecord;
import io.krabka.streams.schema.KrabkaSchemaRegistryClient;
import io.krabka.streams.schema.ProtobufSerde;
import io.krabka.streams.schema.SchemaCache;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;

class ProtobufBatchCodecTest {
    @Test
    void decodesFramedMessagesIntoTypedColumnsAndEncodesThemBack() {
        var descriptor = TestProtos.everything();
        var cache = new SchemaCache(new KrabkaSchemaRegistryClient(URI.create("http://127.0.0.1:1")));
        cache.seedSubjectId("events-value", 12);
        cache.seedWriterSchema(12, "syntax = \"proto3\";");
        cache.seedWriterMessageType(12, descriptor.getFullName());
        var serde = ProtobufSerde.forValue(DynamicMessage.getDefaultInstance(descriptor), cache);
        var message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), "e-1")
                .setField(descriptor.findFieldByName("count"), 3)
                .addRepeatedField(descriptor.findFieldByName("tags"), "x")
                .build();
        var frame = serde.serializer().serialize("events", message);

        try (var allocator = new RootAllocator()) {
            var codec = ProtobufBatchCodec.of(
                    DynamicMessage.getDefaultInstance(descriptor), cache, allocator);
            var records = List.of(new ConsumedRecord(
                    "k".getBytes(StandardCharsets.UTF_8), frame, 42, 0, 9));

            try (var batch = codec.decode("events", records)) {
                assertThat(new String(((VarCharVector) batch.getVector("id")).get(0)))
                        .isEqualTo("e-1");
                assertThat(((BigIntVector) batch.getVector("__timestamp")).get(0)).isEqualTo(42);
                assertThat(codec.arrowSchema().getFields())
                        .hasSize(descriptor.getFields().size());

                var produced = codec.encode("events", batch);

                assertThat(produced).hasSize(1);
                assertThat(produced.get(0).value()).isEqualTo(frame);
            }
        }
    }
}
