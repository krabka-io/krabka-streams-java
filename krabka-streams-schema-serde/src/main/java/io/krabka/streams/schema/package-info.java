/**
 * Confluent-wire-compatible Avro, Protobuf, and JSON Schema serdes.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var client = new KrabkaSchemaRegistryClient(URI.create("http://localhost:8081"));
 * var cache = new SchemaCache(client);
 * var serde = JsonSchemaSerde.forValue(Order.class, orderSchema, cache, true);
 * serde.registerSubject("orders");
 * cache.prewarm().join();
 * }</pre>
 */
package io.krabka.streams.schema;
