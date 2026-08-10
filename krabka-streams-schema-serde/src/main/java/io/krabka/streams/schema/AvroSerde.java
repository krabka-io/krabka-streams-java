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
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

/**
 * A Kafka serde for Confluent-framed Avro values.
 *
 * <p>Three data models are supported: generated {@link SpecificRecord} classes
 * ({@link #forValue(Class, SchemaCache)} and {@link #forKey(Class, SchemaCache)}),
 * schema-driven {@link GenericRecord} values ({@link #generic(Schema, SchemaCache, Role)}),
 * and ordinary Java classes through Avro reflection
 * ({@link #reflect(Class, SchemaCache, Role)}). All of them frame records with the
 * Confluent wire format and resolve schema IDs through a shared {@link SchemaCache}.
 *
 * <p>Deserialization performs full Avro schema resolution: the record is read with
 * the writer schema registered under the frame's schema ID and projected onto this
 * serde's reader schema, so old and new record versions interoperate within Avro's
 * compatibility rules.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var client = new KrabkaSchemaRegistryClient(URI.create("http://localhost:8081"));
 * var cache = new SchemaCache(client);
 *
 * var serde = AvroSerde.forValue(Order.class, cache);
 * serde.registerSubject("orders");
 * cache.prewarm().join();
 *
 * byte[] bytes = serde.serializer().serialize("orders", order);
 * Order roundTripped = serde.deserializer().deserialize("orders", bytes);
 * }</pre>
 *
 * @param <T> the Avro record type carried by the serde
 */
public final class AvroSerde<T> extends AbstractSchemaSerde<T> {
    private final Schema readerSchema;
    private final Function<Schema, DatumWriter<T>> writerFactory;
    private final BiFunction<Schema, Schema, DatumReader<T>> readerFactory;

    private AvroSerde(
            SchemaCache cache,
            Role role,
            Schema readerSchema,
            Function<Schema, DatumWriter<T>> writerFactory,
            BiFunction<Schema, Schema, DatumReader<T>> readerFactory,
            SubjectNameStrategy subjectNameStrategy) {
        super(cache, role, SchemaKind.AVRO, SchemaNormalization.toParsingForm(readerSchema), null, subjectNameStrategy);
        this.readerSchema = readerSchema;
        this.writerFactory = writerFactory;
        this.readerFactory = readerFactory;
    }

    /**
     * Creates a value serde for a generated Avro class.
     *
     * @param <T> the generated Avro record type
     * @param type the generated class, whose embedded schema becomes the reader schema
     * @param cache the cache that resolves subjects and writer schemas
     * @return a value serde for {@code type}
     */
    public static <T extends SpecificRecord> AvroSerde<T> forValue(Class<T> type, SchemaCache cache) {
        return specific(type, cache, Role.VALUE, null);
    }

    /**
     * Creates a value serde for a generated Avro class with a custom subject strategy.
     *
     * @param <T> the generated Avro record type
     * @param type the generated class, whose embedded schema becomes the reader schema
     * @param cache the cache that resolves subjects and writer schemas
     * @param strategy the subject naming strategy that overrides the cache default
     * @return a value serde for {@code type}
     */
    public static <T extends SpecificRecord> AvroSerde<T> forValue(
            Class<T> type, SchemaCache cache, SubjectNameStrategy strategy) {
        return specific(type, cache, Role.VALUE, strategy);
    }

    /**
     * Creates a key serde for a generated Avro class.
     *
     * @param <T> the generated Avro record type
     * @param type the generated class, whose embedded schema becomes the reader schema
     * @param cache the cache that resolves subjects and writer schemas
     * @return a key serde for {@code type}
     */
    public static <T extends SpecificRecord> AvroSerde<T> forKey(Class<T> type, SchemaCache cache) {
        return specific(type, cache, Role.KEY, null);
    }

    /**
     * Creates a key serde for a generated Avro class with a custom subject strategy.
     *
     * @param <T> the generated Avro record type
     * @param type the generated class, whose embedded schema becomes the reader schema
     * @param cache the cache that resolves subjects and writer schemas
     * @param strategy the subject naming strategy that overrides the cache default
     * @return a key serde for {@code type}
     */
    public static <T extends SpecificRecord> AvroSerde<T> forKey(
            Class<T> type, SchemaCache cache, SubjectNameStrategy strategy) {
        return specific(type, cache, Role.KEY, strategy);
    }

    /**
     * Creates a serde for schema-driven {@link GenericRecord} values.
     *
     * @param schema the reader schema for every record
     * @param cache the cache that resolves subjects and writer schemas
     * @param role whether the serde handles record keys or values
     * @return a generic-record serde for {@code schema}
     */
    public static AvroSerde<GenericRecord> generic(Schema schema, SchemaCache cache, Role role) {
        return generic(schema, cache, role, null);
    }

    /**
     * Creates a {@link GenericRecord} serde with a custom subject strategy.
     *
     * @param schema the reader schema for every record
     * @param cache the cache that resolves subjects and writer schemas
     * @param role whether the serde handles record keys or values
     * @param strategy the subject naming strategy that overrides the cache default
     * @return a generic-record serde for {@code schema}
     */
    public static AvroSerde<GenericRecord> generic(
            Schema schema, SchemaCache cache, Role role, SubjectNameStrategy strategy) {
        return new AvroSerde<>(
                cache, role, schema, GenericDatumWriter::new, GenericDatumReader::new, strategy);
    }

    /**
     * Creates a serde backed by Avro reflection for ordinary Java classes.
     *
     * <p>The reader schema is derived from the class with {@link ReflectData}, so the
     * class needs no generated code and no Avro annotations for simple shapes.
     *
     * @param <T> the reflected Java type
     * @param type the Java class to derive the schema from
     * @param cache the cache that resolves subjects and writer schemas
     * @param role whether the serde handles record keys or values
     * @return a reflection-based serde for {@code type}
     */
    public static <T> AvroSerde<T> reflect(
            Class<T> type, SchemaCache cache, Role role) {
        return reflect(type, cache, role, null);
    }

    /**
     * Creates a reflection-based serde with a custom subject strategy.
     *
     * @param <T> the reflected Java type
     * @param type the Java class to derive the schema from
     * @param cache the cache that resolves subjects and writer schemas
     * @param role whether the serde handles record keys or values
     * @param strategy the subject naming strategy that overrides the cache default
     * @return a reflection-based serde for {@code type}
     */
    public static <T> AvroSerde<T> reflect(
            Class<T> type, SchemaCache cache, Role role, SubjectNameStrategy strategy) {
        var reflectData = ReflectData.get();
        var schema = reflectData.getSchema(type);
        return new AvroSerde<>(
                cache,
                role,
                schema,
                writer -> new ReflectDatumWriter<>(writer, reflectData),
                (writer, reader) -> new ReflectDatumReader<>(writer, reader, reflectData),
                strategy);
    }

    private static <T extends SpecificRecord> AvroSerde<T> specific(
            Class<T> type, SchemaCache cache, Role role, SubjectNameStrategy strategy) {
        var schema = SpecificData.get().getSchema(type);
        return new AvroSerde<>(
                cache, role, schema, SpecificDatumWriter::new, SpecificDatumReader::new, strategy);
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
        var parser = new Schema.Parser();
        cache().writerReferences(schemaId).values().forEach(parser::parse);
        var writerSchema = parser.parse(cache().writerSchema(schemaId));
        var decoder = DecoderFactory.get().binaryDecoder(body, null);
        return readerFactory.apply(writerSchema, readerSchema).read(null, decoder);
    }
}
