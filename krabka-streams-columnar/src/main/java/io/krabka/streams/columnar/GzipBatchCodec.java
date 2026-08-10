package io.krabka.streams.columnar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Adds per-record GZIP compression to any batch codec.
 *
 * <p>Encoding compresses each produced record's value after the delegate has encoded
 * it; decoding decompresses each fetched record's value before the delegate sees it.
 * Keys, timestamps, and headers pass through unchanged. Decompression is capped to
 * guard against decompression bombs: a record that inflates past
 * {@code maxUncompressedBytes} throws {@link ColumnarException}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var codec = new GzipBatchCodec(new BlobCodec(allocator));
 * topology.addSource("source", List.of("compressed-transactions"), codec);
 * }</pre>
 */
public final class GzipBatchCodec implements BatchCodec {
    /** The default decompression ceiling: 16 MiB per record. */
    public static final int DEFAULT_MAX_UNCOMPRESSED_BYTES = 16 * 1024 * 1024;

    private final BatchCodec delegate;
    private final int maxUncompressedBytes;

    /**
     * Wraps a codec with the {@value #DEFAULT_MAX_UNCOMPRESSED_BYTES}-byte ceiling.
     *
     * @param delegate the codec that handles the uncompressed record values
     */
    public GzipBatchCodec(BatchCodec delegate) {
        this(delegate, DEFAULT_MAX_UNCOMPRESSED_BYTES);
    }

    /**
     * Wraps a codec with an explicit decompression ceiling.
     *
     * @param delegate the codec that handles the uncompressed record values
     * @param maxUncompressedBytes the largest value a record may decompress to
     * @throws IllegalArgumentException if the ceiling is not positive or is
     *     {@link Integer#MAX_VALUE}
     */
    public GzipBatchCodec(BatchCodec delegate, int maxUncompressedBytes) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (maxUncompressedBytes < 1 || maxUncompressedBytes == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "maxUncompressedBytes must be between 1 and " + (Integer.MAX_VALUE - 1));
        }
        this.maxUncompressedBytes = maxUncompressedBytes;
    }

    /**
     * Decompresses record values, then decodes them with an empty topic name.
     *
     * @param records the records of one topic partition batch
     * @return the delegate's decoded batch; the caller must close it
     */
    @Override
    public VectorSchemaRoot decode(List<ConsumedRecord> records) {
        return decode("", records);
    }

    /**
     * Decompresses record values, then delegates decoding.
     *
     * @param topic the topic the records were fetched from
     * @param records the records of one topic partition batch
     * @return the delegate's decoded batch; the caller must close it
     * @throws ColumnarException if a value is not valid GZIP or inflates past the ceiling
     */
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

    /**
     * Encodes with an empty topic name, then compresses each record value.
     *
     * @param batch the batch to encode; the codec reads it and leaves it open
     * @return the compressed records in the delegate's order
     */
    @Override
    public List<ProduceRecord> encode(VectorSchemaRoot batch) {
        return encode("", batch);
    }

    /**
     * Delegates encoding, then compresses each record value.
     *
     * @param topic the topic the records will be produced to
     * @param batch the batch to encode; the codec reads it and leaves it open
     * @return the compressed records in the delegate's order
     */
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
