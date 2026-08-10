package io.krabka.streams.columnar.schema;

import com.google.protobuf.Message;
import io.krabka.streams.columnar.BatchCodec;
import io.krabka.streams.columnar.ConsumedRecord;
import io.krabka.streams.columnar.ProduceRecord;
import io.krabka.streams.columnar.RowCodec;
import io.krabka.streams.schema.ProtobufSerde;
import io.krabka.streams.schema.SchemaCache;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Decodes registry-framed Protobuf topics into Arrow batches and encodes them back.
 *
 * <p>The codec composes a {@link ProtobufSerde} with a {@link ProtobufRowBridge}:
 * each record value is unframed and parsed with the message type's descriptor, then
 * written as one row whose columns follow the descriptor — nested messages, repeated
 * fields, maps, and {@code google.protobuf.Timestamp} as native Arrow types. The
 * reserved {@code __key}, {@code __timestamp}, {@code __partition}, {@code __offset},
 * and {@code __headers} columns are appended as for every
 * {@link io.krabka.streams.columnar.RowCodec}. The Arrow schema derives from the
 * local descriptor alone, so a new writer schema mid-stream never changes the
 * columns; unknown fields are dropped by the descriptor-driven rebuild.
 *
 * <p>Subjects resolve through the shared {@link SchemaCache}: call
 * {@link #registerSubject(String)} for every topic the codec reads or writes, then
 * prewarm the cache before processing.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var codec = ProtobufBatchCodec.of(Order.getDefaultInstance(), cache, allocator);
 * codec.registerSubject("orders");
 * cache.prewarm().join();
 *
 * var topology = new ColumnarTopology(allocator);
 * var source = topology.addSource("orders", List.of("orders"), codec);
 * }</pre>
 */
public final class ProtobufBatchCodec implements BatchCodec {
    private final ProtobufSerde<?> serde;
    private final BatchCodec delegate;
    private final org.apache.arrow.vector.types.pojo.Schema arrowSchema;

    private <T extends Message> ProtobufBatchCodec(
            ProtobufSerde<T> serde, ProtobufRowBridge<T> bridge, BufferAllocator allocator) {
        this.serde = serde;
        this.delegate = new RowCodec<>(serde, bridge, allocator);
        this.arrowSchema = bridge.arrowSchema();
    }

    /**
     * Creates a codec for one message type.
     *
     * @param <T> the message type
     * @param defaultInstance the default instance of the message type, for example
     *     {@code Order.getDefaultInstance()}
     * @param cache the cache that resolves subjects and writer schemas
     * @param allocator the allocator that owns decoded batches
     * @return a codec whose columns follow the message descriptor
     */
    public static <T extends Message> ProtobufBatchCodec of(
            T defaultInstance, SchemaCache cache, BufferAllocator allocator) {
        Objects.requireNonNull(defaultInstance, "defaultInstance");
        Objects.requireNonNull(cache, "cache");
        Objects.requireNonNull(allocator, "allocator");
        return new ProtobufBatchCodec(
                ProtobufSerde.forValue(defaultInstance, cache),
                ProtobufRowBridge.of(defaultInstance),
                allocator);
    }

    /**
     * Interns the message schema for a topic's value subject in the shared cache.
     *
     * <p>Call this once per topic before prewarming, exactly as with the underlying
     * serde.
     *
     * @param topic the topic whose subject resolves the message schema
     */
    public void registerSubject(String topic) {
        serde.registerSubject(topic);
    }

    /**
     * Returns the payload Arrow schema of decoded batches, without the reserved
     * metadata columns.
     *
     * @return the Arrow schema derived from the message descriptor
     */
    public org.apache.arrow.vector.types.pojo.Schema arrowSchema() {
        return arrowSchema;
    }

    /**
     * Decodes records with an empty topic name.
     *
     * @param records the records of one topic partition batch
     * @return the decoded batch; the caller must close it
     */
    @Override
    public VectorSchemaRoot decode(List<ConsumedRecord> records) {
        return delegate.decode(records);
    }

    /**
     * Decodes records, resolving the subject through the topic name.
     *
     * @param topic the topic the records were fetched from
     * @param records the records of one topic partition batch
     * @return the decoded batch, one row per record, with metadata columns appended;
     *     the caller must close it
     */
    @Override
    public VectorSchemaRoot decode(String topic, List<ConsumedRecord> records) {
        return delegate.decode(topic, records);
    }

    /**
     * Encodes a batch with an empty topic name.
     *
     * @param batch the batch to encode; the codec reads it and leaves it open
     * @return the encoded records in row order
     */
    @Override
    public List<ProduceRecord> encode(VectorSchemaRoot batch) {
        return delegate.encode(batch);
    }

    /**
     * Encodes a batch into one registry-framed record per row.
     *
     * @param topic the topic the records will be produced to
     * @param batch the batch to encode; the codec reads it and leaves it open
     * @return the encoded records in row order
     */
    @Override
    public List<ProduceRecord> encode(String topic, VectorSchemaRoot batch) {
        return delegate.encode(topic, batch);
    }
}
