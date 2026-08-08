package io.krabka.streams.schema;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

class ConfluentWireFormatTest {
    @Test
    void encodesMagicBigEndianIdAndBody() {
        var frame = ConfluentWireFormat.encode(258, new byte[] {'x', 'y'});

        assertArrayEquals(new byte[] {0, 0, 0, 1, 2, 'x', 'y'}, frame);
        var decoded = ConfluentWireFormat.decode(frame);
        assertEquals(258, decoded.schemaId());
        assertArrayEquals(new byte[] {'x', 'y'}, decoded.body());
    }

    @Test
    void usesSingleZeroForTopLevelProtobufMessage() {
        var frame = ConfluentWireFormat.encodeProtobuf(7, List.of(0), new byte[] {'p', 'b'});

        assertArrayEquals(new byte[] {0, 0, 0, 0, 7, 0, 'p', 'b'}, frame);
        var decoded = ConfluentWireFormat.decodeProtobuf(frame);
        assertEquals(List.of(0), decoded.messageIndexes());
        assertArrayEquals(new byte[] {'p', 'b'}, decoded.body());
    }

    @Test
    void roundTripsNestedProtobufIndexes() {
        var frame = ConfluentWireFormat.encodeProtobuf(9, List.of(1, 0), new byte[] {3});

        var decoded = ConfluentWireFormat.decodeProtobuf(frame);

        assertEquals(9, decoded.schemaId());
        assertEquals(List.of(1, 0), decoded.messageIndexes());
        assertArrayEquals(new byte[] {3}, decoded.body());
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
