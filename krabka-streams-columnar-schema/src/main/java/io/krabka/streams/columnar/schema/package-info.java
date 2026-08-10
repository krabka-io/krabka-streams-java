/**
 * Avro and Protobuf bridges into the Arrow columnar runtime.
 *
 * <p>This package connects the schema registry serdes to columnar topologies. The
 * batch codecs, {@link io.krabka.streams.columnar.schema.AvroBatchCodec} and
 * {@link io.krabka.streams.columnar.schema.ProtobufBatchCodec}, decode
 * registry-framed records into Arrow batches whose columns follow the record schema
 * — structs, lists, maps, decimals, and timestamps arrive as native Arrow types
 * instead of the JSON text fallback of
 * {@link io.krabka.streams.columnar.JsonRowBridge}. The row bridges,
 * {@link io.krabka.streams.columnar.schema.AvroRowBridge} and
 * {@link io.krabka.streams.columnar.schema.ProtobufRowBridge}, expose the same
 * conversion through the {@link io.krabka.streams.columnar.RowBridge} interface for
 * composition with your own serdes, and
 * {@link io.krabka.streams.columnar.schema.AvroArrowSchemas} and
 * {@link io.krabka.streams.columnar.schema.ProtobufArrowSchemas} translate schemas
 * without touching data.
 *
 * <p>Every bridge derives its Arrow schema once, at construction, from the fixed
 * reader schema or message descriptor. Records written with other registered writer
 * schemas are resolved onto that reader view by the embedded serde, so every batch
 * of a topology carries the same Arrow schema regardless of mid-stream schema
 * evolution.
 */
package io.krabka.streams.columnar.schema;
