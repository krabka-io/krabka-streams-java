package io.krabka.streams.columnar.schema;

import io.krabka.streams.columnar.BatchCodec;
import io.krabka.streams.columnar.ConsumedRecord;
import io.krabka.streams.columnar.ProduceRecord;
import io.krabka.streams.columnar.RowCodec;
import io.krabka.streams.schema.AvroSerde;
import io.krabka.streams.schema.Role;
import io.krabka.streams.schema.SchemaCache;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;

/**
 * Decodes registry-framed Avro topics into Arrow batches and encodes them back.
 *
 * <p>The codec composes an {@link AvroSerde} with an {@link AvroRowBridge}: each
 * record value is unframed, resolved from its writer schema onto the fixed reader
 * schema, and written as one row whose columns follow the reader schema —
 * structs, lists, maps, decimals, and timestamps as native Arrow types. The reserved
 * {@code __key}, {@code __timestamp}, {@code __partition}, {@code __offset}, and
 * {@code __headers} columns are appended as for every
 * {@link io.krabka.streams.columnar.RowCodec}. Because the Arrow schema derives from
 * the reader schema alone, a new writer schema mid-stream never changes the
 * columns.
 *
 * <p>Subjects resolve through the shared {@link SchemaCache}: call
 * {@link #registerSubject(String)} for every topic the codec reads or writes, then
 * prewarm the cache before processing. An unknown writer schema ID during decode
 * starts one background fetch and throws the cache's retriable
 * {@code SchemaFetchPendingException}, exactly as ordinary consumers experience.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var codec = AvroBatchCodec.generic(orderSchema, cache, allocator);
 * codec.registerSubject("orders");
 * cache.prewarm().join();
 *
 * var topology = new ColumnarTopology(allocator);
 * var source = topology.addSource("orders", List.of("orders"), codec);
 * }</pre>
 */
public final class AvroBatchCodec implements BatchCodec {
    private final AvroSerde<?> serde;
    private final BatchCodec delegate;
    private final org.apache.arrow.vector.types.pojo.Schema arrowSchema;

    private <T extends IndexedRecord> AvroBatchCodec(
            AvroSerde<T> serde, AvroRowBridge<T> bridge, BufferAllocator allocator) {
        this.serde = serde;
        this.delegate = new RowCodec<>(serde, bridge, allocator);
        this.arrowSchema = bridge.arrowSchema();
    }

    /**
     * Creates a codec for schema-driven {@link GenericRecord} values.
     *
     * @param schema the reader schema for every record; must be a record schema
     * @param cache the cache that resolves subjects and writer schemas
     * @param allocator the allocator that owns decoded batches
     * @return a codec whose columns follow {@code schema}
     */
    public static AvroBatchCodec generic(Schema schema, SchemaCache cache, BufferAllocator allocator) {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(cache, "cache");
        Objects.requireNonNull(allocator, "allocator");
        return new AvroBatchCodec(
                AvroSerde.generic(schema, cache, Role.VALUE), AvroRowBridge.generic(schema), allocator);
    }

    /**
     * Creates a codec for a generated Avro class.
     *
     * @param <T> the generated Avro record type
     * @param type the generated class, whose embedded schema drives the columns
     * @param cache the cache that resolves subjects and writer schemas
     * @param allocator the allocator that owns decoded batches
     * @return a codec whose columns follow the embedded schema of {@code type}
     */
    public static <T extends SpecificRecord> AvroBatchCodec forValue(
            Class<T> type, SchemaCache cache, BufferAllocator allocator) {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(cache, "cache");
        Objects.requireNonNull(allocator, "allocator");
        return new AvroBatchCodec(
                AvroSerde.forValue(type, cache), AvroRowBridge.forSpecific(type), allocator);
    }

    /**
     * Interns the reader schema for a topic's value subject in the shared cache.
     *
     * <p>Call this once per topic before prewarming, exactly as with the underlying
     * serde.
     *
     * @param topic the topic whose subject resolves the reader schema
     */
    public void registerSubject(String topic) {
        serde.registerSubject(topic);
    }

    /**
     * Returns the payload Arrow schema of decoded batches, without the reserved
     * metadata columns.
     *
     * @return the Arrow schema derived from the reader schema
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
     * Decodes records, resolving writer schemas through the topic's subject.
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
