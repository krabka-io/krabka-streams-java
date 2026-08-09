package io.krabka.streams.columnar;

import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Converts one partition batch between Kafka records and Arrow vectors. */
public interface BatchCodec {
    VectorSchemaRoot decode(List<ConsumedRecord> records);

    default VectorSchemaRoot decode(String topic, List<ConsumedRecord> records) {
        return decode(records);
    }

    List<ProduceRecord> encode(VectorSchemaRoot batch);

    default List<ProduceRecord> encode(String topic, VectorSchemaRoot batch) {
        return encode(batch);
    }
}
