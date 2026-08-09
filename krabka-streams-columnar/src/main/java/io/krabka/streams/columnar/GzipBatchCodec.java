package io.krabka.streams.columnar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Adds per-record GZIP compression to any batch codec. */
public final class GzipBatchCodec implements BatchCodec {
    public static final int DEFAULT_MAX_UNCOMPRESSED_BYTES = 16 * 1024 * 1024;
    private final BatchCodec delegate;
    private final int maxUncompressedBytes;

    public GzipBatchCodec(BatchCodec delegate) {
        this(delegate, DEFAULT_MAX_UNCOMPRESSED_BYTES);
    }

    public GzipBatchCodec(BatchCodec delegate, int maxUncompressedBytes) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (maxUncompressedBytes < 1 || maxUncompressedBytes == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "maxUncompressedBytes must be between 1 and " + (Integer.MAX_VALUE - 1));
        }
        this.maxUncompressedBytes = maxUncompressedBytes;
    }

    @Override
    public VectorSchemaRoot decode(List<ConsumedRecord> records) {
        return decode("", records);
    }

    @Override
    public VectorSchemaRoot decode(String topic, List<ConsumedRecord> records) {
        return delegate.decode(topic, records.stream()
                .map(record -> new ConsumedRecord(
                        record.key(),
                        decompress(record.value()),
                        record.timestamp(),
                        record.partition(),
                        record.offset(),
                        record.headers()))
                .toList());
    }

    @Override
    public List<ProduceRecord> encode(VectorSchemaRoot batch) {
        return encode("", batch);
    }

    @Override
    public List<ProduceRecord> encode(String topic, VectorSchemaRoot batch) {
        return delegate.encode(topic, batch).stream()
                .map(record -> new ProduceRecord(
                        record.key(), compress(record.value()), record.timestamp(), record.headers()))
                .toList();
    }

    private byte[] decompress(byte[] compressed) {
        try (var input = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            var result = input.readNBytes(maxUncompressedBytes + 1);
            if (result.length > maxUncompressedBytes) {
                throw new ColumnarException(
                        "GZIP record exceeds maxUncompressedBytes=" + maxUncompressedBytes);
            }
            return result;
        } catch (IOException error) {
            throw new ColumnarException("cannot decompress GZIP record", error);
        }
    }

    private static byte[] compress(byte[] uncompressed) {
        try {
            var result = new ByteArrayOutputStream();
            try (var output = new GZIPOutputStream(result)) {
                output.write(uncompressed);
            }
            return result.toByteArray();
        } catch (IOException error) {
            throw new AssertionError(error);
        }
    }
}
