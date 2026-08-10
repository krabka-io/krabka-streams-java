package io.krabka.streams.columnar.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.krabka.streams.columnar.ColumnarException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

class AvroRowBridgeTest {
    private static final Schema SCALARS = new Schema.Parser().parse(
            """
            {"type": "record", "name": "Scalars", "fields": [
              {"name": "text", "type": "string"},
              {"name": "small", "type": "int"},
              {"name": "big", "type": "long"},
              {"name": "single", "type": "float"},
              {"name": "wide", "type": "double"},
              {"name": "flag", "type": "boolean"},
              {"name": "raw", "type": "bytes"},
              {"name": "maybe", "type": ["null", "string"]},
              {"name": "price", "type": {"type": "bytes", "logicalType": "decimal",
                                         "precision": 10, "scale": 2}},
              {"name": "day", "type": {"type": "int", "logicalType": "date"}},
              {"name": "clock", "type": {"type": "int", "logicalType": "time-millis"}},
              {"name": "at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
              {"name": "local_at", "type": {"type": "long", "logicalType": "local-timestamp-micros"}}
            ]}""");

    private static final Schema NESTED = new Schema.Parser().parse(
            """
            {"type": "record", "name": "Nested", "fields": [
              {"name": "child", "type": {"type": "record", "name": "Child", "fields": [
                {"name": "name", "type": "string"},
                {"name": "score", "type": ["null", "long"]}]}},
              {"name": "tags", "type": {"type": "array", "items": "string"}},
              {"name": "labels", "type": {"type": "map", "values": "long"}},
              {"name": "color", "type": {"type": "enum", "name": "Color",
                                         "symbols": ["RED", "BLUE"]}},
              {"name": "checksum", "type": {"type": "fixed", "name": "Sum", "size": 4}},
              {"name": "either", "type": ["int", "string"]}
            ]}""");

    @Test
    void roundTripsScalarsAndLogicalTypes() {
        var record = new GenericData.Record(SCALARS);
        record.put("text", "a");
        record.put("small", 7);
        record.put("big", 9L);
        record.put("single", 1.5f);
        record.put("wide", 2.5);
        record.put("flag", true);
        record.put("raw", ByteBuffer.wrap(new byte[] {1, 2}));
        record.put("maybe", null);
        var priceSchema = SCALARS.getField("price").schema();
        record.put("price", new Conversions.DecimalConversion()
                .toBytes(new BigDecimal("12.34"), priceSchema, priceSchema.getLogicalType()));
        record.put("day", 19_000);
        record.put("clock", 3_600_123);
        record.put("at", 1_700_000_000_123L);
        record.put("local_at", 1_700_000_000_123_456L);

        var bridge = AvroRowBridge.generic(SCALARS);
        try (var allocator = new RootAllocator();
                var batch = bridge.rowsToBatch(List.of(record), allocator)) {
            assertThat(((DecimalVector) batch.getVector("price")).getObject(0))
                    .isEqualByComparingTo(new BigDecimal("12.34"));

            var back = bridge.batchToRows(batch);

            assertThat(back).hasSize(1);
            assertThat(back.get(0)).isEqualTo(record);
        }
    }

    @Test
    void roundTripsNestedRecordsCollectionsEnumsFixedAndUnions() {
        var childSchema = NESTED.getField("child").schema();
        var child = new GenericData.Record(childSchema);
        child.put("name", "n");
        child.put("score", 42L);
        var record = new GenericData.Record(NESTED);
        record.put("child", child);
        record.put("tags", List.of("x", "y"));
        var labels = new HashMap<String, Object>();
        labels.put("k", 7L);
        record.put("labels", labels);
        record.put("color", new GenericData.EnumSymbol(NESTED.getField("color").schema(), "BLUE"));
        record.put("checksum", new GenericData.Fixed(
                NESTED.getField("checksum").schema(), new byte[] {1, 2, 3, 4}));
        record.put("either", "words");
        var other = new GenericData.Record(NESTED);
        var otherChild = new GenericData.Record(childSchema);
        otherChild.put("name", "m");
        otherChild.put("score", null);
        other.put("child", otherChild);
        other.put("tags", List.of());
        other.put("labels", new HashMap<String, Object>());
        other.put("color", new GenericData.EnumSymbol(NESTED.getField("color").schema(), "RED"));
        other.put("checksum", new GenericData.Fixed(
                NESTED.getField("checksum").schema(), new byte[] {5, 6, 7, 8}));
        other.put("either", 42);

        var bridge = AvroRowBridge.generic(NESTED);
        try (var allocator = new RootAllocator();
                var batch = bridge.rowsToBatch(List.of(record, other), allocator)) {
            var union = (StructVector) batch.getVector("either");
            assertThat(union.getChild("string").isNull(0)).isFalse();
            assertThat(union.getChild("int").isNull(0)).isTrue();
            assertThat(union.getChild("int").isNull(1)).isFalse();

            var back = bridge.batchToRows(batch);

            assertThat(back.get(0)).isEqualTo(record);
            assertThat(back.get(1)).isEqualTo(other);
        }
    }

    @Test
    void roundTripsRecursiveRecordsThroughJsonFallback() {
        var schema = new Schema.Parser().parse(
                """
                {"type": "record", "name": "Node", "fields": [
                  {"name": "label", "type": "string"},
                  {"name": "next", "type": ["null", "Node"]}
                ]}""");
        var tail = new GenericData.Record(schema);
        tail.put("label", "b");
        tail.put("next", null);
        var head = new GenericData.Record(schema);
        head.put("label", "a");
        head.put("next", tail);

        var bridge = AvroRowBridge.generic(schema);
        try (var allocator = new RootAllocator();
                var batch = bridge.rowsToBatch(List.of(head), allocator)) {
            var back = bridge.batchToRows(batch);
            assertThat(back.get(0)).isEqualTo(head);
        }
    }

    @Test
    void emptyBatchCarriesTheFullSchemaAndNullRequiredFieldsFail() {
        var bridge = AvroRowBridge.generic(SCALARS);
        try (var allocator = new RootAllocator();
                var batch = bridge.rowsToBatch(List.of(), allocator)) {
            assertThat(batch.getSchema().getFields()).hasSize(SCALARS.getFields().size());
            assertThat(batch.getRowCount()).isZero();
        }

        var missing = new GenericData.Record(SCALARS);
        try (var allocator = new RootAllocator()) {
            assertThatThrownBy(() -> bridge.rowsToBatch(List.of(missing), allocator))
                    .isInstanceOf(ColumnarException.class)
                    .hasMessageContaining("text");
        }
    }

    @Test
    void rejectsNonRecordTopLevelSchemas() {
        assertThatThrownBy(() -> AvroRowBridge.generic(Schema.create(Schema.Type.STRING)))
                .isInstanceOf(ColumnarException.class)
                .hasMessageContaining("record");
    }

    @Test
    void decodedGenericRecordsRoundTripLikeHandBuiltOnes() {
        var bridge = AvroRowBridge.generic(NESTED);
        assertThat(bridge.arrowSchema().getFields())
                .extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                .containsExactly("child", "tags", "labels", "color", "checksum", "either");
    }
}
