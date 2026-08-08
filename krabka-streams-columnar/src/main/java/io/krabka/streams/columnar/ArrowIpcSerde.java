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

/** A Kafka serde for Arrow IPC stream batches. The caller must close decoded roots. */
public final class ArrowIpcSerde implements Serde<VectorSchemaRoot> {
    private final BufferAllocator allocator;
    private final Serializer<VectorSchemaRoot> serializer = new IpcSerializer();
    private final Deserializer<VectorSchemaRoot> deserializer = new IpcDeserializer();

    public ArrowIpcSerde(BufferAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
    }

    @Override
    public Serializer<VectorSchemaRoot> serializer() {
        return serializer;
    }

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
