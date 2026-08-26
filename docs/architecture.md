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
      ├── krabka-streams-coordination    kafka-clients, through krabka-streams (api)
      └── krabka-streams-test-utils      depends on all four
                                         kafka-streams-test-utils (api)
```

`krabka-streams` holds one class. Its real job is to be the single place where the
Kafka Streams version is pinned, so the other modules inherit it and applications get a
consistent classpath from any one artifact.

`krabka-streams-coordination` sits beside the feature modules and knows none of them. It
uses the Kafka producer, consumer, and admin clients only, so it runs in a process that
has no Kafka Streams topology and no Arrow.

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

### A barrier cut arrives as a manifest, not as a marker

A barrier marker is a Kafka control record, and the JVM `KafkaConsumer` drops every
control batch before the application sees it. Java cannot see a marker in band,
whatever the runtime does. The broker publishes each cut to the internal
`__barrier_state` topic for that reason, and `BarrierCutReader` reads it with a plain
assign, seek, and poll loop over a consumer the caller owns.

The consequence is that alignment is offset arithmetic, not marker detection. The runner
compares each partition's consumed offsets against the manifest, processes everything
below the marker offset, and holds the rest. A partition that reaches its offset pauses
until the others catch up, so a lagging partition costs no repeated fetches.

The reader drops partial cuts. A partial cut names partitions that will never receive
the epoch's marker, so a task that waited for one would wait forever. The broker
publishes a partial cut, and does not hide it, so that a reader can skip the epoch.

### The leadership epoch is the safety mechanism, and the lease is not

`krabka-streams-coordination` elects one leader per role. The leadership epoch is the
producer epoch that Kafka's transaction coordinator mints for
`transactional.id = <role>`. The quorum mints it, the coordinator never reuses a value,
and every broker rejects a write that carries a superseded epoch. The cluster fences a
deposed leader, and the leader does not have to fence itself.

The lease adds no safety of its own. It decides when a standby stops waiting for a quiet
holder, and nothing else. A wrong lease makes a failover early or late. A wrong lease
never makes two writers authoritative. The consequence for a caller is that a leader
does not prove its lease is live before each write. The write carries the proof, and the
broker checks it.

Two details follow from that choice and are easy to get wrong. The fencing token is the
pair `(producerId, producerEpoch)` and the comparison reads the producer id first,
because the epoch is a `short` that wraps and Kafka then allocates a fresh producer id.
Rank comes from the offset of a registration record in the log, not from a configuration
file, so a recovered node lands at the tail of the roster and never preempts the member
that replaced it.

All records of one role go to one partition, which `RolePartitioner` computes from the
role name with Kafka's own key hash. The registration key and the lease key of one role
differ, so a key-hashing partitioner would split the role across two partitions and
destroy the total order the rank depends on.

See [Coordination](coordination.md).

### Snapshots are keyed by epoch, and the container is shared

`ColumnarStateStore` keys a snapshot by partition and epoch. Rebalance state uses
`LIVE_EPOCH`, which is `-1`, and a barrier epoch is never negative, so one store holds
both without a second interface.

The file container is the layout the barrier design freezes for the three krabka streams
libraries: a big-endian `u32` version, a `u32` entry count, and then length-prefixed
names and values, with entries in ascending byte order of the name. The bytes are
then deterministic, and the three libraries read one another's containers. The
payload inside each entry stays language-specific, so a Java snapshot does not restore
into a Go task.

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
| `CoordinationException`       | coordination codec, client  | malformed record, bad config, or a failed cluster call             |
| `FencedException`             | coordination transport      | another member holds the role now; stop the work of the role       |
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
| `CoordinationCodec`          | thread-safe; a static codec with no state                                       |
| `Succession`, `RoleState`    | thread-safe; pure rules over immutable values                                   |
| `RoleStateBuilder`           | not thread-safe while folding                                                   |
| `KafkaCoordinationTransport` | thread-safe; producer state in `ConcurrentHashMap`                              |
| `Leadership`                 | not thread-safe; confine it to the thread that runs the role                    |
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
| Apache Kafka Clients            | 4.3.1   | `api`, through `krabka-streams`              |
| networknt json-schema-validator | 2.0.4   | `implementation`                             |
| JUnit                           | 5.13.4  | test                                         |

Kafka 4.3.1 is the floor for the streams group protocol. Arrow 19 is the version whose
direct-buffer access requires the `--add-opens` flag.
