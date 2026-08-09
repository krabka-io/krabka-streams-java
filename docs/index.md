# krabka streams for Java documentation

This directory documents `krabka-streams-java` version `1.0.0`.

## Guides

| Document                                    | Contents                                                              |
| ------------------------------------------- | --------------------------------------------------------------------- |
| [Getting started](getting-started.md)       | Requirements, coordinates, and a first application                    |
| [Configuration](configuration.md)           | `KrabkaStreamsConfig`, the streams group protocol, and JVM flags      |
| [Schema registry](schema-registry.md)       | Registry client, schema cache, subjects, and prewarming               |
| [Serdes](serdes.md)                         | Avro, Protobuf, and JSON Schema serdes and the Confluent wire format  |
| [Columnar processing](columnar.md)          | Arrow batches, codecs, topologies, and the partition runner           |
| [Columnar operators](columnar-operators.md) | Built-in operators, custom processors, and buffer ownership           |
| [Testing](testing.md)                       | `ColumnarTestDriver`, `SchemaRegistryStub`, and the integration suite |
| [API reference](api-reference.md)           | Every public type, grouped by module                                  |
| [Architecture](architecture.md)             | Module layout, data flow, and design decisions                        |
| [Runtime constraints](limitations.md)       | Broker, JVM, Arrow, and packaging constraints                         |
| [Troubleshooting](troubleshooting.md)       | Error messages mapped to causes and fixes                             |
| [Build and release](build-and-release.md)   | Gradle tasks, CI workflows, and publishing                            |

## Related files

- [README.md](../README.md): project overview
- [PARITY.md](../PARITY.md): feature parity checklist
- [CHANGELOG.md](../CHANGELOG.md): release notes

## Conventions in these documents

Code samples assume static imports are absent and use `var` where the Java compiler
can infer the type. Samples that allocate Arrow memory use try-with-resources, because
every `VectorSchemaRoot` and `BufferAllocator` owns off-heap memory that the caller
must release. See [buffer ownership](columnar-operators.md#buffer-ownership) for the rules.
