package io.krabka.streams.schema;

import java.io.ByteArrayOutputStream;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

/** A Kafka serde for Confluent-framed Avro values. */
public final class AvroSerde<T> extends AbstractSchemaSerde<T> {
    private final Schema readerSchema;
    private final Function<Schema, DatumWriter<T>> writerFactory;
    private final BiFunction<Schema, Schema, DatumReader<T>> readerFactory;

    private AvroSerde(
            SchemaCache cache,
            Role role,
            Schema readerSchema,
            Function<Schema, DatumWriter<T>> writerFactory,
            BiFunction<Schema, Schema, DatumReader<T>> readerFactory) {
        super(cache, role, SchemaKind.AVRO, SchemaNormalization.toParsingForm(readerSchema), null);
        this.readerSchema = readerSchema;
        this.writerFactory = writerFactory;
        this.readerFactory = readerFactory;
    }

    public static <T extends SpecificRecord> AvroSerde<T> forValue(Class<T> type, SchemaCache cache) {
        return specific(type, cache, Role.VALUE);
    }

    public static <T extends SpecificRecord> AvroSerde<T> forKey(Class<T> type, SchemaCache cache) {
        return specific(type, cache, Role.KEY);
    }

    public static AvroSerde<GenericRecord> generic(Schema schema, SchemaCache cache, Role role) {
        return new AvroSerde<>(cache, role, schema, GenericDatumWriter::new, GenericDatumReader::new);
    }

    private static <T extends SpecificRecord> AvroSerde<T> specific(Class<T> type, SchemaCache cache, Role role) {
        var schema = SpecificData.get().getSchema(type);
        return new AvroSerde<>(cache, role, schema, SpecificDatumWriter::new, SpecificDatumReader::new);
    }

    @Override
    protected byte[] serializeBody(T value) throws Exception {
        var output = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(output, null);
        writerFactory.apply(readerSchema).write(value, encoder);
        encoder.flush();
        return output.toByteArray();
    }

    @Override
    protected T deserializeBody(int schemaId, byte[] body) throws Exception {
        var writerSchema = new Schema.Parser().parse(cache().writerSchema(schemaId));
        var decoder = DecoderFactory.get().binaryDecoder(body, null);
        return readerFactory.apply(writerSchema, readerSchema).read(null, decoder);
    }
}
