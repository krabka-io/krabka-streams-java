package io.krabka.streams.columnar.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

class ProtobufArrowSchemasTest {
    @Test
    void mapsScalarsWithUnsignedPolicy() {
        var arrow = ProtobufArrowSchemas.toArrowSchema(TestProtos.everything());

        assertThat(type(arrow, "id")).isEqualTo(new ArrowType.Utf8());
        assertThat(type(arrow, "count")).isEqualTo(new ArrowType.Int(32, true));
        assertThat(type(arrow, "ucount")).isEqualTo(new ArrowType.Int(64, true));
        assertThat(type(arrow, "big_count")).isEqualTo(new ArrowType.Int(64, false));
        assertThat(type(arrow, "ratio")).isEqualTo(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        assertThat(type(arrow, "flag")).isEqualTo(new ArrowType.Bool());
        assertThat(type(arrow, "payload")).isEqualTo(new ArrowType.Binary());
        assertThat(field(arrow, "id").isNullable()).isFalse();
    }

    @Test
    void mapsEnumsMessagesRepeatedAndMaps() {
        var arrow = ProtobufArrowSchemas.toArrowSchema(TestProtos.everything());

        assertThat(type(arrow, "color")).isEqualTo(new ArrowType.Utf8());
        assertThat(field(arrow, "color").getMetadata())
                .containsEntry("krabka.proto.enum", "krabka.test.Color");
        assertThat(type(arrow, "chld")).isEqualTo(new ArrowType.Struct());
        assertThat(field(arrow, "chld").isNullable()).isTrue();
        assertThat(type(arrow, "tags")).isEqualTo(new ArrowType.List());
        assertThat(field(arrow, "tags").isNullable()).isFalse();
        assertThat(type(arrow, "labels")).isEqualTo(new ArrowType.Map(false));
        var entries = field(arrow, "labels").getChildren().get(0);
        assertThat(entries.getChildren().get(0).getType()).isEqualTo(new ArrowType.Utf8());
        assertThat(entries.getChildren().get(1).getType()).isEqualTo(new ArrowType.Int(64, true));
    }

    @Test
    void mapsWellKnownTypesAndOneofs() {
        var arrow = ProtobufArrowSchemas.toArrowSchema(TestProtos.everything());

        assertThat(type(arrow, "stamp")).isEqualTo(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
        assertThat(field(arrow, "stamp").isNullable()).isTrue();
        assertThat(type(arrow, "maybe_name")).isEqualTo(new ArrowType.Utf8());
        assertThat(field(arrow, "maybe_name").isNullable()).isTrue();
        assertThat(field(arrow, "maybe_name").getMetadata())
                .containsEntry("krabka.proto.wrapper", "google.protobuf.StringValue");
        assertThat(type(arrow, "meta")).isEqualTo(new ArrowType.Utf8());
        assertThat(field(arrow, "meta").getMetadata())
                .containsEntry("krabka.json", "true")
                .containsEntry("krabka.proto.message", "google.protobuf.Struct");
        for (var name : new String[] {"either_text", "either_num"}) {
            assertThat(field(arrow, name).isNullable()).isTrue();
            assertThat(field(arrow, name).getMetadata()).containsEntry("krabka.proto.oneof", "either");
        }
    }

    @Test
    void recursiveMessagesFallBackToJsonText() {
        var arrow = ProtobufArrowSchemas.toArrowSchema(TestProtos.child());

        assertThat(type(arrow, "next")).isEqualTo(new ArrowType.Utf8());
        assertThat(field(arrow, "next").getMetadata())
                .containsEntry("krabka.json", "true")
                .containsEntry("krabka.proto.message", "krabka.test.Child");
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
