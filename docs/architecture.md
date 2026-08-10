# Architecture

## Modules

```text
krabka-streams                 org.apache.kafka:kafka-streams (api)
      ▲
      ├── krabka-streams-schema-serde    avro, protobuf-java, jackson-databind (api)
      │                                  json-schema-validator (implementation)
      ├── krabka-streams-columnar        arrow-vector (api), jackson-databind (implementation)
      │                                  arrow-memory-netty (runtimeOnly)
      ├── krabka-streams-columnar-schema krabka-streams-schema-serde and
      │                                  krabka-streams-columnar (api),
      │                                  protobuf-java-util (implementation)
      └── krabka-streams-test-utils      depends on all four
                                         kafka-streams-test-utils (api)
```

`krabka-streams` holds one class. Its real job is to be the single place where the
Kafka Streams version is pinned, so the other modules inherit it and applications get a
consistent classpath from any one artifact.

The two base feature modules do not know about each other: the schema module has no
Arrow dependency and the columnar module has no Avro or Protobuf dependency. They meet
in exactly two deliberate places. `krabka-streams-columnar-schema` is the production
bridge — its batch codecs and row bridges map registry schemas onto native Arrow
columns — and `krabka-streams-test-utils` is the testing one. Applications that use
neither Avro nor Protobuf in columnar topologies never load the bridge module.

Dependencies are declared with `api` where the types appear in public signatures
(`Serde`, `VectorSchemaRoot`, `Schema`, `Message`, `ObjectMapper`) and with
`implementation` or `runtimeOnly` where they do not. That is why the JSON Schema
validator and the Arrow Netty allocator stay off your compile classpath.

## Design decisions

### The Kafka Streams API is re-exported, not wrapped

There is no `KrabkaStreams` class, no builder facade, no shadow DSL. Applications use
`StreamsBuilder`, `Topology`, `KafkaStreams`, `Stores`, and `StateQueryRequest`
directly. The only krabka-specific call is `KrabkaStreamsConfig.withDefaults`, and even
that is a `Properties` transformation you can inspect and override.

Upstream documentation and examples therefore apply unchanged, and upgrading Kafka
Streams does not require re-deriving a wrapper. `KafkaStreamsParityTest` exists to prove
the exported surface works, not to add anything on top of it.

### Schema resolution is asynchronous; serdes are synchronous

Kafka's serializer contract is synchronous, and a registry lookup is an HTTP call.
Doing that lookup inside `serialize` would block a stream thread. The split here is
explicit:

- `KrabkaSchemaRegistryClient` returns `CompletableFuture` and never blocks.
- `SchemaCache` is a plain in-memory map that serdes read synchronously.
- `prewarm()` is the one place where the two meet, and you choose when to call it.
  Normally that is before `KafkaStreams.start()`, where a failure is a startup failure.

The residual case is a consumer meeting an unknown writer schema ID mid-stream. That
resolves to a single background fetch plus `SchemaFetchPendingException`, which extends
Kafka's `RetriableException` so the existing retry machinery handles it. Concurrent
callers for the same ID share one in-flight request, and a failed fetch clears its
marker so the next attempt retries cleanly.

The consequence to plan for: the first record carrying a new schema ID always fails
once. That is deliberate, on the view that a failed-and-retried record costs less than a
blocked stream thread.

### The columnar model is a separate runtime

`krabka-streams-columnar` does not build a Kafka `Topology`. It has its own node graph,
consumer-group runner, partition state lifecycle, and test driver. Mixing the two
models in one application means running them side by side, not composing them.

Keeping them separate keeps the semantics visible. A Kafka Streams operator sees one
record and Kafka-managed stores; a columnar operator sees one fetched batch and
partition-local state managed by `BuiltColumnarTopology`. Snapshots are explicit and
the file store is local rather than a broker changelog.

### One fetched batch is the unit of work

Arrow pays off when a vector is long enough to amortize per-batch overhead, and a
consumer fetch is the natural place where many records already arrive together. Making
the fetch the processing unit means there is no buffering layer, no batch-assembly
timer, and no extra latency knob. Batch size is whatever `max.poll.records` and the
fetch settings already produce.

It also fixes the memory profile: at most one batch per node is live at a time, and
everything is released when `runBatch` returns.

### Metadata travels as columns

Kafka record metadata is projected into five reserved columns rather than kept in a
side structure. Operators then need no special API to filter on partition, sort by
offset, or carry keys through a projection, because it is all column access. The cost is
five reserved names, enforced at decode time with a clear error.

Under `BlobCodec`, where one record expands into many rows, each row keeps the
metadata of the record it came from. `__offset` is therefore the link back from a row
to its source record. `__headers` preserves ordered, duplicate, and null-valued headers.

### Ownership is explicit because the memory is off-heap

Arrow buffers are reference-counted and not managed by the garbage collector. Rather
than hide that, the API states who closes what: the framework owns batches inside
`runBatch`, callers own whatever a public method returns to them, and forwarding a
batch transfers ownership. `BuiltColumnarTopology` closes every intermediate batch in a
`finally` block, deduplicating by identity so a forwarded input is not double-closed.

Closing a `RootAllocator` with outstanding buffers throws, which turns a leak into a
loud test failure instead of slow off-heap growth in production.

### Defensive copies at the boundary

`ConsumedRecord`, `ProduceRecord`, and both wire-format frames copy their byte arrays on
construction and again on every accessor call. Consumer buffers get reused by the Kafka
client, and Arrow-adjacent code holds references longer than a single callback, so
aliasing those arrays would be a real hazard. The copies cost extra allocation, which is
the price of avoiding it.

### Validation happens at build time

`ColumnarTopology.validate()` checks names, parents, and the presence of a source and a
sink before any data flows. Parents must already exist when a child is added, which
makes cycles unrepresentable rather than merely detected. `build()` returns a separate
type, `BuiltColumnarTopology`, so a validated topology is distinguishable from a
half-built one in the type system.

### Errors are typed and specific

| Exception                     | Raised by                   | Meaning                                                            |
| ----------------------------- | --------------------------- | ------------------------------------------------------------------ |
| `SchemaRegistryException`     | registry client             | transport, status, or response problem; `statusCode()` tells which |
| `SchemaFetchPendingException` | `SchemaCache`               | a writer schema is being fetched; retry                            |
| `SerializationException`      | serdes, `ArrowIpcSerde`     | Kafka's own type, so existing handlers apply                       |
| `ColumnarException`           | codecs, topology, operators | Arrow-side failure                                                 |
| `IllegalArgumentException`    | builders                    | programming error caught at wiring time                            |

Messages name the offending element, whether that is the subject, the column, the node,
or the record index, so a failure identifies itself without a debugger.

## Data flow

### Kafka Streams path

```text
settings ──► KrabkaStreamsConfig.withDefaults ──► KafkaStreams
                                                       │
                                          streams group protocol (KIP-1071)
                                                       │
    Serde ◄── SchemaCache ◄── prewarm ◄── KrabkaSchemaRegistryClient ──► registry
```

### Columnar path

```text
consumer.poll ──► ConsumedRecord[] ──► source (BatchCodec.decode)
                                              │
                                        VectorSchemaRoot
                                              │
                                    operator (copy in, batches out)
                                              │
                                    sink (BatchCodec.encode)
                                              │
                       ProducedToTopic[] ──► producer.send ──► acknowledgements
```

## Concurrency

| Type                         | Safety                                                                          |
| ---------------------------- | ------------------------------------------------------------------------------- |
| `KrabkaSchemaRegistryClient` | thread-safe; stateless over an `HttpClient`                                     |
| `SchemaCache`                | thread-safe; all state in `ConcurrentHashMap`                                   |
| Serdes                       | thread-safe once the cache is prewarmed; the JSON validator cache is concurrent |
| Batch codecs and row bridges | thread-safe; the Arrow schema is fixed at construction and rows copy on convert |
| `ColumnarTopology`           | not thread-safe while building                                                  |
| `BuiltColumnarTopology`      | thread-safe; serialized calls and state isolated by logical partition           |
| `ColumnarTestDriver`         | not thread-safe                                                                 |
| `SchemaRegistryStub`         | request handling is synchronized                                                |
| Arrow allocators and roots   | not thread-safe; confine to one thread                                          |

One built topology can be shared when serialized processing is acceptable. Use one per
thread for parallelism and independent processor state.

## Version pinning

| Dependency                      | Version | Scope                                        |
| ------------------------------- | ------- | -------------------------------------------- |
| Apache Kafka Streams            | 4.3.1   | `api`                                        |
| Apache Avro                     | 1.12.1  | `api`                                        |
| Protobuf Java                   | 4.33.5  | `api`                                        |
| Protobuf Java Util              | 4.33.5  | `implementation`                             |
| Jackson Databind                | 2.22.0  | `api` (serde), `implementation` (columnar)   |
| Apache Arrow                    | 19.0.0  | `api` (vector), `runtimeOnly` (memory-netty) |
| networknt json-schema-validator | 2.0.4   | `implementation`                             |
| JUnit                           | 5.13.4  | test                                         |

Kafka 4.3.1 is the floor for the streams group protocol. Arrow 19 is the version whose
direct-buffer access requires the `--add-opens` flag.
