# Runtime constraints

The project-owned limitations listed for `1.2.0` have been resolved. The remaining
constraints come from the runtime or the services the library interoperates with.

## Kafka Streams broker support

`group.protocol=streams` requires a broker with the streams rebalance protocol enabled
and `streams.version=1` finalized. Use `group.protocol=classic` with an older broker.
`KrabkaStreamsConfig.withDefaults` preserves either explicit choice.

State stores, punctuators, interactive queries, and processing guarantees are Apache
Kafka Streams APIs and retain the behavior of the Kafka version exported by this
library.

## Java and Arrow

Java 17 is the minimum supported runtime. Published classes target `--release 17`.
Artifacts carry stable `Automatic-Module-Name` entries for JPMS consumers.

Arrow 19 needs this runtime option when it accesses direct buffers:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

## Packaging

The `io.krabka:krabka-streams-bom` platform pins every module to one release. Each
module also publishes an `all` classifier containing its runtime dependencies. Use the
ordinary artifact when your dependency manager already controls those libraries; use
the shaded artifact only for a standalone classpath.

The `all` artifacts do not relocate third-party package names. Relocation would break
service loading, generated Avro and Protobuf types, and Arrow's reflective access, so
dependency-managed ordinary artifacts remain the default.
