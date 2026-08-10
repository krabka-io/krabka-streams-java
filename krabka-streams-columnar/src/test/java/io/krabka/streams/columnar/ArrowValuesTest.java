package io.krabka.streams.columnar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

class ArrowValuesTest {
    @Test
    void writesAndReadsTimeColumnsAsLocalTimeOrNumbers() {
        var time = LocalTime.of(13, 30, 5, 123_456_789);
        try (var allocator = new RootAllocator();
                var root = ArrowValues.createRoot(
                        List.of(
                                field("sec", new ArrowType.Time(TimeUnit.SECOND, 32)),
                                field("milli", new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
                                field("micro", new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
                                field("nano", new ArrowType.Time(TimeUnit.NANOSECOND, 64))),
                        2,
                        allocator)) {
            for (var name : List.of("sec", "milli", "micro", "nano")) {
                ArrowValues.set(root.getVector(name), 0, time);
                ArrowValues.set(root.getVector(name), 1, null);
            }
            ArrowValues.finish(root);

            assertThat(ArrowValues.get(root.getVector("sec"), 0)).isEqualTo(time.toSecondOfDay());
            assertThat(ArrowValues.get(root.getVector("milli"), 0))
                    .isEqualTo(LocalTime.of(13, 30, 5, 123_000_000));
            assertThat(ArrowValues.get(root.getVector("micro"), 0))
                    .isEqualTo(time.toNanoOfDay() / 1_000L);
            assertThat(ArrowValues.get(root.getVector("nano"), 0)).isEqualTo(time.toNanoOfDay());
            for (var name : List.of("sec", "milli", "micro", "nano")) {
                assertThat(ArrowValues.get(root.getVector(name), 1)).isNull();
            }
        }
    }

    @Test
    void writesFixedSizeBinaryAndRejectsWrongLength() {
        try (var allocator = new RootAllocator();
                var root = ArrowValues.createRoot(
                        List.of(field("fixed", new ArrowType.FixedSizeBinary(4))), 2, allocator)) {
            ArrowValues.set(root.getVector("fixed"), 0, new byte[] {1, 2, 3, 4});
            ArrowValues.set(root.getVector("fixed"), 1, ByteBuffer.wrap(new byte[] {5, 6, 7, 8}));
            ArrowValues.finish(root);

            assertThat(ArrowValues.get(root.getVector("fixed"), 0))
                    .isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
            assertThat(ArrowValues.get(root.getVector("fixed"), 1))
                    .isEqualTo(ByteBuffer.wrap(new byte[] {5, 6, 7, 8}));
            assertThatThrownBy(() -> ArrowValues.set(root.getVector("fixed"), 0, new byte[] {1}))
                    .isInstanceOf(ColumnarException.class)
                    .hasMessageContaining("4 bytes");
        }
    }

    @Test
    void readsUnsigned64BitValuesExactly() {
        var aboveSignedRange = new BigInteger("18446744073709551615");
        try (var allocator = new RootAllocator();
                var root = ArrowValues.createRoot(
                        List.of(field("count", new ArrowType.Int(64, false))), 1, allocator)) {
            ArrowValues.set(root.getVector("count"), 0, aboveSignedRange);
            ArrowValues.finish(root);

            assertThat(ArrowValues.get(root.getVector("count"), 0)).isEqualTo(aboveSignedRange);
        }
    }

    @Test
    void roundTripsScalarsThroughTheFacade() {
        try (var allocator = new RootAllocator();
                var root = ArrowValues.createRoot(
                        List.of(field("user", new ArrowType.Utf8()), field("amount", new ArrowType.Int(64, true))),
                        1,
                        allocator)) {
            ArrowValues.set(root.getVector("user"), 0, "ada");
            ArrowValues.set(root.getVector("amount"), 0, 7L);
            ArrowValues.finish(root);

            assertThat(ArrowValues.get(root.getVector("user"), 0)).isEqualTo("ada");
            assertThat(ArrowValues.get(root.getVector("amount"), 0)).isEqualTo(7L);
        }
    }

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }
}
