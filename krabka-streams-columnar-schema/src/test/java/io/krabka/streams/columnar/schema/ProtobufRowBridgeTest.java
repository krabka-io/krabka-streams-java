package io.krabka.streams.columnar.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import java.math.BigInteger;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;

class ProtobufRowBridgeTest {
    @Test
    void roundTripsEveryFieldShape() {
        var descriptor = TestProtos.everything();
        var childDescriptor = TestProtos.child();
        var grandchild = DynamicMessage.newBuilder(childDescriptor)
                .setField(childDescriptor.findFieldByName("name"), "m")
                .build();
        var child = DynamicMessage.newBuilder(childDescriptor)
                .setField(childDescriptor.findFieldByName("name"), "n")
                .setField(childDescriptor.findFieldByName("next"), grandchild)
                .build();
        var labelsDescriptor = descriptor.findNestedTypeByName("LabelsEntry");
        var label = DynamicMessage.newBuilder(labelsDescriptor)
                .setField(labelsDescriptor.findFieldByName("key"), "k")
                .setField(labelsDescriptor.findFieldByName("value"), 7L)
                .build();
        var message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), "a")
                .setField(descriptor.findFieldByName("count"), -5)
                .setField(descriptor.findFieldByName("ucount"), -1) // 4294967295 as raw bits
                .setField(descriptor.findFieldByName("big_count"), -1L) // 2^64-1 as raw bits
                .setField(descriptor.findFieldByName("ratio"), 2.5)
                .setField(descriptor.findFieldByName("flag"), true)
                .setField(descriptor.findFieldByName("payload"), ByteString.copyFrom(new byte[] {1, 2}))
                .setField(
                        descriptor.findFieldByName("color"),
                        descriptor.findFieldByName("color").getEnumType().findValueByName("BLUE"))
                .setField(descriptor.findFieldByName("chld"), child)
                .addRepeatedField(descriptor.findFieldByName("tags"), "x")
                .addRepeatedField(descriptor.findFieldByName("tags"), "y")
                .addRepeatedField(descriptor.findFieldByName("labels"), label)
                .setField(
                        descriptor.findFieldByName("stamp"),
                        Timestamp.newBuilder().setSeconds(12).setNanos(345_678_000).build())
                .setField(
                        descriptor.findFieldByName("maybe_name"),
                        com.google.protobuf.StringValue.of("w"))
                .setField(
                        descriptor.findFieldByName("meta"),
                        Struct.newBuilder()
                                .putFields("deep", Value.newBuilder().setStringValue("v").build())
                                .build())
                .setField(descriptor.findFieldByName("either_text"), "t")
                .build();

        var bridge = ProtobufRowBridge.of(DynamicMessage.getDefaultInstance(descriptor));
        try (var allocator = new RootAllocator();
                var batch = bridge.rowsToBatch(List.of(message), allocator)) {
            assertThat(((UInt8Vector) batch.getVector("big_count")).getObjectNoOverflow(0))
                    .isEqualTo(new BigInteger("18446744073709551615"));
            assertThat(new String(((VarCharVector) batch.getVector("color")).get(0)))
                    .isEqualTo("BLUE");

            var back = bridge.batchToRows(batch);

            assertThat(back).hasSize(1);
            assertThat(back.get(0).toByteArray()).isEqualTo(message.toByteArray());
        }
    }

    @Test
    void unsetPresenceFieldsStayUnsetAndImplicitScalarsKeepDefaults() {
        var descriptor = TestProtos.everything();
        var message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), "only")
                .build();

        var bridge = ProtobufRowBridge.of(DynamicMessage.getDefaultInstance(descriptor));
        try (var allocator = new RootAllocator();
                var batch = bridge.rowsToBatch(List.of(message), allocator)) {
            assertThat(batch.getVector("chld").isNull(0)).isTrue();
            assertThat(batch.getVector("stamp").isNull(0)).isTrue();
            assertThat(batch.getVector("maybe_name").isNull(0)).isTrue();
            assertThat(batch.getVector("either_text").isNull(0)).isTrue();
            assertThat(batch.getVector("either_num").isNull(0)).isTrue();
            assertThat(batch.getVector("count").isNull(0)).isFalse();

            var back = bridge.batchToRows(batch);

            assertThat(back.get(0).hasField(descriptor.findFieldByName("chld"))).isFalse();
            assertThat(back.get(0).hasField(descriptor.findFieldByName("either_text"))).isFalse();
            assertThat(back.get(0).getField(descriptor.findFieldByName("count"))).isEqualTo(0);
            assertThat(back.get(0).toByteArray()).isEqualTo(message.toByteArray());
        }
    }

    @Test
    void truncatesTimestampNanosBelowOneMicrosecond() {
        var descriptor = TestProtos.everything();
        var message = DynamicMessage.newBuilder(descriptor)
                .setField(
                        descriptor.findFieldByName("stamp"),
                        Timestamp.newBuilder().setSeconds(-2).setNanos(345_678_901).build())
                .build();

        var bridge = ProtobufRowBridge.of(DynamicMessage.getDefaultInstance(descriptor));
        try (var allocator = new RootAllocator();
                var batch = bridge.rowsToBatch(List.of(message), allocator)) {
            var back = bridge.batchToRows(batch);
            var stamp = (com.google.protobuf.Message) back.get(0)
                    .getField(descriptor.findFieldByName("stamp"));
            var stampDescriptor = stamp.getDescriptorForType();
            assertThat(stamp.getField(stampDescriptor.findFieldByName("seconds"))).isEqualTo(-2L);
            assertThat(stamp.getField(stampDescriptor.findFieldByName("nanos"))).isEqualTo(345_678_000);
        }
    }

    @Test
    void unknownEnumNumbersRoundTripAsDigitStrings() {
        var descriptor = TestProtos.everything();
        var colorField = descriptor.findFieldByName("color");
        var message = DynamicMessage.newBuilder(descriptor)
                .setField(colorField, colorField.getEnumType().findValueByNumberCreatingIfUnknown(7))
                .build();

        var bridge = ProtobufRowBridge.of(DynamicMessage.getDefaultInstance(descriptor));
        try (var allocator = new RootAllocator();
                var batch = bridge.rowsToBatch(List.of(message), allocator)) {
            assertThat(new String(((VarCharVector) batch.getVector("color")).get(0))).isEqualTo("7");

            var back = bridge.batchToRows(batch);

            var symbol = (com.google.protobuf.Descriptors.EnumValueDescriptor)
                    back.get(0).getField(colorField);
            assertThat(symbol.getNumber()).isEqualTo(7);
        }
    }

    @Test
    void emptyBatchCarriesTheFullSchema() {
        var bridge = ProtobufRowBridge.of(DynamicMessage.getDefaultInstance(TestProtos.everything()));
        try (var allocator = new RootAllocator();
                var batch = bridge.rowsToBatch(List.of(), allocator)) {
            assertThat(batch.getSchema().getFields())
                    .hasSize(TestProtos.everything().getFields().size());
            assertThat(batch.getRowCount()).isZero();
        }
    }
}
