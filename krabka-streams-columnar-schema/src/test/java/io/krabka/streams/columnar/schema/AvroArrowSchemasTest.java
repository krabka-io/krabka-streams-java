package io.krabka.streams.columnar.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.krabka.streams.columnar.ColumnarException;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

class AvroArrowSchemasTest {
    @Test
    void mapsScalarsAndLogicalTypes() {
        var schema = new Schema.Parser().parse(
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
                  {"name": "huge", "type": {"type": "fixed", "name": "Big", "size": 32,
                                            "logicalType": "decimal", "precision": 40, "scale": 4}},
                  {"name": "token", "type": {"type": "string", "logicalType": "uuid"}},
                  {"name": "day", "type": {"type": "int", "logicalType": "date"}},
                  {"name": "clock", "type": {"type": "int", "logicalType": "time-millis"}},
                  {"name": "fine_clock", "type": {"type": "long", "logicalType": "time-micros"}},
                  {"name": "at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                  {"name": "at_micro", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                  {"name": "local_at", "type": {"type": "long", "logicalType": "local-timestamp-millis"}}
                ]}""");

        var arrow = AvroArrowSchemas.toArrowSchema(schema);

        assertThat(type(arrow, "text")).isEqualTo(new ArrowType.Utf8());
        assertThat(type(arrow, "small")).isEqualTo(new ArrowType.Int(32, true));
        assertThat(type(arrow, "big")).isEqualTo(new ArrowType.Int(64, true));
        assertThat(type(arrow, "single")).isEqualTo(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        assertThat(type(arrow, "wide")).isEqualTo(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        assertThat(type(arrow, "flag")).isEqualTo(new ArrowType.Bool());
        assertThat(type(arrow, "raw")).isEqualTo(new ArrowType.Binary());
        assertThat(field(arrow, "text").isNullable()).isFalse();
        assertThat(field(arrow, "maybe").isNullable()).isTrue();
        assertThat(type(arrow, "maybe")).isEqualTo(new ArrowType.Utf8());
        assertThat(type(arrow, "price")).isEqualTo(new ArrowType.Decimal(10, 2, 128));
        assertThat(type(arrow, "huge")).isEqualTo(new ArrowType.Decimal(40, 4, 256));
        assertThat(type(arrow, "token")).isEqualTo(new ArrowType.Utf8());
        assertThat(field(arrow, "token").getMetadata()).containsEntry("krabka.avro.logical", "uuid");
        assertThat(type(arrow, "day")).isEqualTo(new ArrowType.Date(DateUnit.DAY));
        assertThat(type(arrow, "clock")).isEqualTo(new ArrowType.Time(TimeUnit.MILLISECOND, 32));
        assertThat(type(arrow, "fine_clock")).isEqualTo(new ArrowType.Time(TimeUnit.MICROSECOND, 64));
        assertThat(type(arrow, "at")).isEqualTo(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
        assertThat(type(arrow, "at_micro")).isEqualTo(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
        assertThat(type(arrow, "local_at")).isEqualTo(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));
    }

    @Test
    void mapsNestedRecordsArraysMapsEnumsAndFixed() {
        var schema = new Schema.Parser().parse(
                """
                {"type": "record", "name": "Nested", "fields": [
                  {"name": "child", "type": {"type": "record", "name": "Child", "fields": [
                    {"name": "name", "type": "string"}]}},
                  {"name": "tags", "type": {"type": "array", "items": "string"}},
                  {"name": "labels", "type": {"type": "map", "values": "long"}},
                  {"name": "color", "type": {"type": "enum", "name": "Color",
                                             "symbols": ["RED", "BLUE"]}},
                  {"name": "checksum", "type": {"type": "fixed", "name": "Sum", "size": 4}}
                ]}""");

        var arrow = AvroArrowSchemas.toArrowSchema(schema);

        assertThat(type(arrow, "child")).isEqualTo(new ArrowType.Struct());
        assertThat(field(arrow, "child").getChildren()).hasSize(1);
        assertThat(field(arrow, "child").getChildren().get(0).getName()).isEqualTo("name");
        assertThat(type(arrow, "tags")).isEqualTo(new ArrowType.List());
        assertThat(field(arrow, "tags").getChildren().get(0).getType()).isEqualTo(new ArrowType.Utf8());
        assertThat(type(arrow, "labels")).isEqualTo(new ArrowType.Map(false));
        var entries = field(arrow, "labels").getChildren().get(0);
        assertThat(entries.getChildren().get(0).getName()).isEqualTo("key");
        assertThat(entries.getChildren().get(1).getType()).isEqualTo(new ArrowType.Int(64, true));
        assertThat(type(arrow, "color")).isEqualTo(new ArrowType.Utf8());
        assertThat(field(arrow, "color").getMetadata())
                .containsEntry("krabka.avro.enum", "Color")
                .containsEntry("krabka.avro.enum.symbols", "RED,BLUE");
        assertThat(type(arrow, "checksum")).isEqualTo(new ArrowType.FixedSizeBinary(4));
        assertThat(field(arrow, "checksum").getMetadata()).containsEntry("krabka.avro.fixed", "Sum");
    }

    @Test
    void spreadsGeneralUnionsIntoStructOfBranches() {
        var schema = new Schema.Parser().parse(
                """
                {"type": "record", "name": "WithUnion", "fields": [
                  {"name": "either", "type": ["int", "string"]},
                  {"name": "maybe_either", "type": ["null", "int", "string"]}
                ]}""");

        var arrow = AvroArrowSchemas.toArrowSchema(schema);

        var either = field(arrow, "either");
        assertThat(either.getType()).isEqualTo(new ArrowType.Struct());
        assertThat(either.getMetadata()).containsEntry("krabka.avro.union", "true");
        assertThat(either.isNullable()).isFalse();
        assertThat(either.getChildren()).extracting(Field::getName).containsExactly("int", "string");
        assertThat(either.getChildren()).allMatch(Field::isNullable);
        assertThat(field(arrow, "maybe_either").isNullable()).isTrue();
    }

    @Test
    void recursiveRecordsFallBackToJsonText() {
        var schema = new Schema.Parser().parse(
                """
                {"type": "record", "name": "Node", "fields": [
                  {"name": "label", "type": "string"},
                  {"name": "next", "type": ["null", "Node"]}
                ]}""");

        var arrow = AvroArrowSchemas.toArrowSchema(schema);

        assertThat(type(arrow, "next")).isEqualTo(new ArrowType.Utf8());
        assertThat(field(arrow, "next").getMetadata()).containsEntry("krabka.json", "true");
        assertThat(field(arrow, "next").isNullable()).isTrue();
    }

    @Test
    void rejectsUnsupportedShapes() {
        var tooPrecise = SchemaBuilder.record("Money").fields()
                .name("amount")
                .type(org.apache.avro.LogicalTypes.decimal(80, 2)
                        .addToSchema(Schema.create(Schema.Type.BYTES)))
                .noDefault()
                .endRecord();
        assertThatThrownBy(() -> AvroArrowSchemas.toArrowSchema(tooPrecise))
                .isInstanceOf(ColumnarException.class)
                .hasMessageContaining("precision");

        var nullField = SchemaBuilder.record("Nothing").fields()
                .name("void").type(Schema.create(Schema.Type.NULL)).noDefault()
                .endRecord();
        assertThatThrownBy(() -> AvroArrowSchemas.toArrowSchema(nullField))
                .isInstanceOf(ColumnarException.class)
                .hasMessageContaining("null");
    }

    @Test
    void nonRecordTopLevelBecomesValueColumn() {
        var arrow = AvroArrowSchemas.toArrowSchema(Schema.create(Schema.Type.STRING));
        assertThat(arrow.getFields()).hasSize(1);
        assertThat(arrow.getFields().get(0).getName()).isEqualTo("value");
        assertThat(arrow.getFields().get(0).getType()).isEqualTo(new ArrowType.Utf8());
    }

    private static Field field(org.apache.arrow.vector.types.pojo.Schema schema, String name) {
        return schema.getFields().stream()
                .filter(candidate -> candidate.getName().equals(name))
                .findFirst()
                .orElseThrow();
    }

    private static ArrowType type(org.apache.arrow.vector.types.pojo.Schema schema, String name) {
        return field(schema, name).getType();
    }
}
