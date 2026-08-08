package io.krabka.streams.columnar;

import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Converts one partition batch between Kafka records and Arrow vectors. */
public interface BatchCodec {
    VectorSchemaRoot decode(List<ConsumedRecord> records);

    List<ProduceRecord> encode(VectorSchemaRoot batch);
}
