package io.krabka.streams.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

class ConfluentWireFormatTest {
    @Test
    void encodesMagicBigEndianIdAndBody() {
        var frame = ConfluentWireFormat.encode(258, new byte[] {'x', 'y'});

        assertThat(frame).containsExactly(0, 0, 0, 1, 2, 'x', 'y');
        assertThat(ConfluentWireFormat.decode(frame))
                .usingRecursiveComparison()
                .isEqualTo(new ConfluentWireFormat.Frame(258, new byte[] {'x', 'y'}));
    }

    @Test
    void usesSingleZeroForTopLevelProtobufMessage() {
        var frame = ConfluentWireFormat.encodeProtobuf(7, List.of(0), new byte[] {'p', 'b'});

        assertThat(frame).containsExactly(0, 0, 0, 0, 7, 0, 'p', 'b');
        assertThat(ConfluentWireFormat.decodeProtobuf(frame))
                .usingRecursiveComparison()
                .isEqualTo(new ConfluentWireFormat.ProtobufFrame(7, List.of(0), new byte[] {'p', 'b'}));
    }

    @Test
    void roundTripsNestedProtobufIndexes() {
        var frame = ConfluentWireFormat.encodeProtobuf(9, List.of(1, 0), new byte[] {3});

        assertThat(ConfluentWireFormat.decodeProtobuf(frame))
                .usingRecursiveComparison()
                .isEqualTo(new ConfluentWireFormat.ProtobufFrame(9, List.of(1, 0), new byte[] {3}));
    }

    @Test
    void rejectsShortAndInvalidFrames() {
        assertThrows(SerializationException.class, () -> ConfluentWireFormat.decode(new byte[4]));
        assertThrows(
                SerializationException.class,
                () -> ConfluentWireFormat.decode(new byte[] {1, 0, 0, 0, 1}));
        assertThrows(
                SerializationException.class,
                () -> ConfluentWireFormat.decodeProtobuf(new byte[] {0, 0, 0, 0, 1}));
    }
}
