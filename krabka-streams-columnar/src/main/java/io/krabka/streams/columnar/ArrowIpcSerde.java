package io.krabka.streams.columnar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A Kafka serde for Arrow IPC stream batches. The caller must close decoded roots.
 *
 * <p>Each record value is one complete Arrow IPC stream containing exactly one record
 * batch. This is the value format {@link BlobCodec} reads and writes; use the serde
 * directly with plain Kafka producers and consumers that exchange Arrow batches
 * without a columnar topology. The serde only handles record values: configuring the
 * deserializer for keys throws {@link IllegalArgumentException}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var serde = new ArrowIpcSerde(allocator);
 * var producer = new KafkaProducer<>(config, new ByteArraySerializer(), serde.serializer());
 * producer.send(new ProducerRecord<>("arrow-batches", batch));
 *
 * // consumer side: every deserialized root must be closed
 * try (VectorSchemaRoot received = serde.deserializer().deserialize("arrow-batches", bytes)) {
 *     process(received);
 * }
 * }</pre>
 */
public final class ArrowIpcSerde implements Serde<VectorSchemaRoot> {
    private final BufferAllocator allocator;
    private final Serializer<VectorSchemaRoot> serializer = new IpcSerializer();
    private final Deserializer<VectorSchemaRoot> deserializer = new IpcDeserializer();

    /**
     * Creates a serde whose deserialized batches are owned by an allocator.
     *
     * @param allocator the allocator that owns every deserialized batch's buffers
     */
    public ArrowIpcSerde(BufferAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
    }

    /**
     * Returns the serializer half of this serde.
     *
     * <p>The serializer writes one Arrow IPC stream per batch and throws
     * {@link SerializationException} when the batch cannot be written. A null batch
     * serializes to null.
     *
     * @return the reusable serializer
     */
    @Override
    public Serializer<VectorSchemaRoot> serializer() {
        return serializer;
    }

    /**
     * Returns the deserializer half of this serde.
     *
     * <p>Every non-null result is a freshly allocated root that the caller must
     * close. Malformed streams and streams without a record batch throw
     * {@link SerializationException}. Null input deserializes to null.
     *
     * @return the reusable deserializer
     */
    @Override
    public Deserializer<VectorSchemaRoot> deserializer() {
        return deserializer;
    }

    byte[] serialize(VectorSchemaRoot root) {
        try {
            var output = new ByteArrayOutputStream();
            try (var writer = new ArrowStreamWriter(root, null, Channels.newChannel(output))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return output.toByteArray();
        } catch (Exception error) {
            throw new SerializationException("cannot write Arrow IPC stream", error);
        }
    }

    VectorSchemaRoot deserialize(byte[] bytes) {
        try (var reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
            if (!reader.loadNextBatch()) {
                throw new SerializationException("Arrow IPC stream has no record batch");
            }
            var source = reader.getVectorSchemaRoot();
            var copy = VectorSchemaRoot.create(source.getSchema(), allocator);
            try (var recordBatch = new VectorUnloader(source).getRecordBatch()) {
                new VectorLoader(copy).load(recordBatch);
            }
            return copy;
        } catch (SerializationException error) {
            throw error;
        } catch (Exception error) {
            throw new SerializationException("cannot read Arrow IPC stream", error);
        }
    }

    private final class IpcSerializer implements Serializer<VectorSchemaRoot> {
        @Override
        public byte[] serialize(String topic, VectorSchemaRoot value) {
            return value == null ? null : ArrowIpcSerde.this.serialize(value);
        }
    }

    private final class IpcDeserializer implements Deserializer<VectorSchemaRoot> {
        @Override
        public VectorSchemaRoot deserialize(String topic, byte[] bytes) {
            return bytes == null ? null : ArrowIpcSerde.this.deserialize(bytes);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            if (isKey) {
                throw new IllegalArgumentException("Arrow IPC batches are record values");
            }
        }
    }
}
