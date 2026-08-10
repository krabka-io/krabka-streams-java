package io.krabka.streams.test;

import static org.assertj.core.api.Assertions.assertThat;

import io.krabka.streams.columnar.BuiltinOp;
import io.krabka.streams.columnar.ColumnarTopology;
import io.krabka.streams.columnar.schema.AvroBatchCodec;
import io.krabka.streams.schema.AvroSerde;
import io.krabka.streams.schema.KrabkaSchemaRegistryClient;
import io.krabka.streams.schema.Role;
import io.krabka.streams.schema.SchemaCache;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

class ColumnarSchemaCodecTest {
    private static final Schema ORDER = new Schema.Parser().parse(
            """
            {"type": "record", "name": "Order", "fields": [
              {"name": "id", "type": "string"},
              {"name": "amount", "type": "long"}
            ]}""");

    @Test
    void runsATopologyOverRegistryFramedAvroRecordsEndToEnd() throws Exception {
        try (var stub = new SchemaRegistryStub(); var allocator = new RootAllocator()) {
            var cache = new SchemaCache(new KrabkaSchemaRegistryClient(stub.uri()));
            var codec = AvroBatchCodec.generic(ORDER, cache, allocator);
            codec.registerSubject("orders");
            codec.registerSubject("large-orders");
            cache.prewarm().join();

            var topology = new ColumnarTopology(allocator);
            var source = topology.addSource("orders", List.of("orders"), codec);
            var filter = topology.addOperator(
                    "large",
                    BuiltinOp.filter(
                            allocator,
                            (batch, row) -> ((BigIntVector) batch.getVector("amount")).get(row) > 4),
                    source);
            topology.addSink("sink", "large-orders", codec, filter);
            var driver = new ColumnarTestDriver(topology.build());

            var serde = AvroSerde.generic(ORDER, cache, Role.VALUE);
            driver.pipeInput("orders", 0, null, frame(serde, "o-1", 9), 100);
            driver.pipeInput("orders", 0, null, frame(serde, "o-2", 1), 101);

            var produced = driver.drainOutput("large-orders");

            assertThat(produced).hasSize(1);
            var decoded = serde.deserializer().deserialize("large-orders", produced.get(0).value());
            assertThat(decoded.get("id").toString()).isEqualTo("o-1");
            assertThat(decoded.get("amount")).isEqualTo(9L);
            assertThat(produced.get(0).timestamp()).isEqualTo(100);
        }
    }

    private static byte[] frame(AvroSerde<org.apache.avro.generic.GenericRecord> serde, String id, long amount) {
        var record = new GenericData.Record(ORDER);
        record.put("id", id);
        record.put("amount", amount);
        return serde.serializer().serialize("orders", record);
    }
}
