package io.krabka.streams.columnar.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.krabka.streams.columnar.ConsumedRecord;
import io.krabka.streams.schema.ConfluentWireFormat;
import io.krabka.streams.schema.KrabkaSchemaRegistryClient;
import io.krabka.streams.schema.SchemaCache;
import io.krabka.streams.schema.SchemaFetchPendingException;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

class AvroBatchCodecTest {
    private static final Schema READER = new Schema.Parser().parse(
            """
            {"type": "record", "name": "Order", "fields": [
              {"name": "id", "type": "string"},
              {"name": "amount", "type": "long"}
            ]}""");

    private static SchemaCache offlineCache() {
        return new SchemaCache(new KrabkaSchemaRegistryClient(URI.create("http://127.0.0.1:1")));
    }

    @Test
    void decodesFramedRecordsIntoTypedColumnsAndEncodesThemBack() {
        var cache = offlineCache();
        cache.seedSubjectId("orders-value", 11);
        cache.seedWriterSchema(11, READER.toString());
        try (var allocator = new RootAllocator()) {
            var codec = AvroBatchCodec.generic(READER, cache, allocator);
            var frame = frame(11, READER, order(READER, "o-1", 7));
            var records = List.of(new ConsumedRecord(
                    "k".getBytes(StandardCharsets.UTF_8), frame, 42, 3, 100));

            try (var batch = codec.decode("orders", records)) {
                assertThat(new String(((VarCharVector) batch.getVector("id")).get(0)))
                        .isEqualTo("o-1");
                assertThat(((BigIntVector) batch.getVector("amount")).get(0)).isEqualTo(7);
                assertThat(new String(((VarBinaryVector) batch.getVector("__key")).get(0)))
                        .isEqualTo("k");
                assertThat(((BigIntVector) batch.getVector("__timestamp")).get(0)).isEqualTo(42);
                assertThat(((BigIntVector) batch.getVector("__offset")).get(0)).isEqualTo(100);

                var produced = codec.encode("orders", batch);

                assertThat(produced).hasSize(1);
                assertThat(produced.get(0).value()).isEqualTo(frame);
                assertThat(produced.get(0).key()).isEqualTo("k".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void resolvesEvolvedWriterSchemasOntoTheFixedReaderColumns() {
        var writer = new Schema.Parser().parse(
                """
                {"type": "record", "name": "Order", "fields": [
                  {"name": "id", "type": "string"},
                  {"name": "amount", "type": "long"},
                  {"name": "note", "type": "string"}
                ]}""");
        var cache = offlineCache();
        cache.seedSubjectId("orders-value", 11);
        cache.seedWriterSchema(11, READER.toString());
        cache.seedWriterSchema(21, writer.toString());
        try (var allocator = new RootAllocator()) {
            var codec = AvroBatchCodec.generic(READER, cache, allocator);
            var evolved = new GenericData.Record(writer);
            evolved.put("id", "o-2");
            evolved.put("amount", 9L);
            evolved.put("note", "dropped by the reader");

            try (var batch = codec.decode("orders", List.of(new ConsumedRecord(
                    null, frame(21, writer, evolved), 1, 0, 0)))) {
                assertThat(batch.getSchema().findField("id")).isNotNull();
                assertThat(batch.getSchema().getFields().stream()
                                .map(org.apache.arrow.vector.types.pojo.Field::getName))
                        .doesNotContain("note");
                assertThat(new String(((VarCharVector) batch.getVector("id")).get(0)))
                        .isEqualTo("o-2");
                assertThat(codec.arrowSchema().getFields()).hasSize(2);
            }
        }
    }

    @Test
    void unknownWriterSchemaIdsThrowTheRetriablePendingFetch() {
        var cache = offlineCache();
        cache.seedSubjectId("orders-value", 11);
        cache.seedWriterSchema(11, READER.toString());
        try (var allocator = new RootAllocator()) {
            var codec = AvroBatchCodec.generic(READER, cache, allocator);
            var unknown = frame(99, READER, order(READER, "o-3", 1));

            assertThatThrownBy(() -> codec.decode(
                            "orders", List.of(new ConsumedRecord(null, unknown, 1, 0, 0))))
                    .hasRootCauseInstanceOf(SchemaFetchPendingException.class);
        }
    }

    private static GenericRecord order(Schema schema, String id, long amount) {
        var record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("amount", amount);
        return record;
    }

    private static byte[] frame(int schemaId, Schema writer, GenericRecord record) {
        try {
            var output = new ByteArrayOutputStream();
            var encoder = EncoderFactory.get().binaryEncoder(output, null);
            new GenericDatumWriter<GenericRecord>(writer).write(record, encoder);
            encoder.flush();
            return ConfluentWireFormat.encode(schemaId, output.toByteArray());
        } catch (java.io.IOException error) {
            throw new AssertionError(error);
        }
    }
}
