# Testing

`krabka-streams-test-utils` bundles everything needed to test all three modules
without a broker or a registry. It depends on `krabka-streams`,
`krabka-streams-schema-serde`, `krabka-streams-columnar`, and Apache Kafka's
`kafka-streams-test-utils`, so one test dependency covers the whole surface.

```kotlin
testImplementation("io.krabka:krabka-streams-test-utils:1.2.0")
```

Any test source set that touches Arrow needs the JVM flag:

```kotlin
tasks.withType<Test>().configureEach {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}
```

## Testing Kafka Streams topologies

Ordinary topologies use Apache Kafka's `TopologyTestDriver`, re-exported by this
artifact. Nothing about krabka changes how it works.

```java
var builder = new StreamsBuilder();
builder.stream("input", Consumed.with(STRINGS, STRINGS))
    .filter((key, value) -> value.startsWith("keep"))
    .groupByKey()
    .count(Materialized.as("counts"))
    .toStream()
    .to("output", Produced.with(STRINGS, Serdes.Long()));

try (var driver = new TopologyTestDriver(builder.build(), properties)) {
  var input = driver.createInputTopic("input", STRINGS.serializer(), STRINGS.serializer());
  var output =
      driver.createOutputTopic("output", STRINGS.deserializer(), Serdes.Long().deserializer());

  input.pipeInput("a", "drop");
  input.pipeInput("a", "keep-one");
  input.pipeInput("a", "keep-two");

  assertThat(output.readKeyValuesToMap()).usingRecursiveComparison().isEqualTo(Map.of("a", 2L));
  assertThat(driver.<String, Long>getKeyValueStore("counts").get("a")).isEqualTo(2L);
}
```

The driver needs only `application.id` and `bootstrap.servers` in its properties; the
bootstrap address is never contacted, so any placeholder works. Do **not** wrap the
properties in `KrabkaStreamsConfig.withDefaults` here, because the group protocol is
meaningless to the test driver.

`KafkaStreamsParityTest` in this repository exercises the re-exported API more widely:
windowed aggregation with `suppress`, stream-stream joins, `GlobalKTable` joins,
versioned state stores, the Processor API, wall-clock punctuators, and IQv2 request
construction. It is a useful catalogue of what the exported API covers.

Advancing time works as it does upstream: pass explicit timestamps to `pipeInput`, or
call `driver.advanceWallClockTime(Duration.ofSeconds(1))` to fire wall-clock
punctuators.

## ColumnarTestDriver

Runs a built columnar topology in-process and queues the produced records per topic.

```java
try (var allocator = new RootAllocator()) {
  var codec = new RowCodec<>(Serdes.String(), new JsonRowBridge<>(String.class), allocator);
  var topology = new ColumnarTopology(allocator);
  var source = topology.addSource("source", List.of("in"), codec);
  topology.addSink("sink", "out", codec, source);
  var driver = new ColumnarTestDriver(topology.build());

  driver.pipeInput("in", 0, bytes("a"), bytes("first"), 10);
  driver.pipeInput("in", 0, bytes("b"), bytes("second"), 11);

  assertThat(driver.outputSize("out")).isEqualTo(2);
  assertThat(driver.readOutput("out"))
      .usingRecursiveComparison()
      .isEqualTo(new ProduceRecord(bytes("a"), bytes("first"), 10));
}
```

| Method                                               | Behavior                                                                                          |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `pipeInput(topic, partition, key, value, timestamp)` | Runs one record as a single-record batch. Offsets start at `0` per topic-partition and increment. |
| `pipeInput(..., headers)`                            | The same operation with an ordered `List<RecordHeader>`.                                          |
| `pipeBatch(topic, records)`                          | Runs a whole `List<ConsumedRecord>` as one batch. Offsets are whatever the records carry.         |
| `failNext(fault)`                                    | Throws one deterministic fault before the next batch evaluation.                                  |
| `outputSize(topic)` / `isOutputEmpty(topic)`         | Queue depth for a sink topic.                                                                     |
| `readOutput(topic)`                                  | Removes and returns the oldest record; throws `NoSuchElementException` when empty.                |
| `drainOutput(topic)`                                 | Removes and returns everything queued for the topic.                                              |

`pipeInput` and `pipeBatch` differ in an important way for `BlobCodec` topologies: each
`pipeInput` call is its own batch, so per-batch operators such as `groupBy` see one
record at a time. Use `pipeBatch` when the test is about batch behavior.

The driver holds no Arrow memory of its own, since every batch is created and closed
inside `runBatch`. The only thing to close is the allocator you created.

## SchemaRegistryStub

A real HTTP server implementing the registry endpoints the client uses, backed by
in-memory state. It binds to `127.0.0.1` on an ephemeral port.

```java
try (var stub = new SchemaRegistryStub()) {
  var client = new KrabkaSchemaRegistryClient(stub.uri());
  var cache = new SchemaCache(client);
  var serde = JsonSchemaSerde.forValue(Order.class, schema, cache, true);

  serde.registerSubject("orders");
  cache.prewarm().join();

  var bytes = serde.serializer().serialize("orders", new Order("o-1"));
  assertThat(serde.deserializer().deserialize("orders", bytes))
      .usingRecursiveComparison()
      .isEqualTo(new Order("o-1"));
  assertThat(stub.requestCount("POST", "/subjects/orders-value/versions")).isOne();
}
```

Implemented endpoints:

| Request                                   | Behavior                                                                                                                            |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `POST /subjects/{subject}/versions`       | Registers the schema, assigning IDs from `1`. Identical schemas reuse an ID; the subject's version list grows. Returns `{"id": n}`. |
| `POST /subjects/{subject}`                | Looks the schema up. `404` with `error_code` `40403` when it is not registered under that subject.                                  |
| `GET /subjects/{subject}/versions/latest` | Returns the newest registration, with `subject` and `version`. `404`/`40401` for an unknown subject.                                |
| `GET /schemas/ids/{id}`                   | Returns the schema for an ID, with `schemaType` and `messageType` when present. `404`/`40403` when unknown.                         |
| anything else                             | `404` with `error_code` `40401`.                                                                                                    |

Schema identity is the triple `(schema, schemaType, messageType)`, so an Avro schema and
a JSON schema with the same text receive different IDs. Malformed request bodies produce
`422` with `error_code` `42201`.

`requestCount(method, path)` counts requests by raw path, which is how you assert that
prewarming resolved a subject exactly once. Handling is synchronized, so counts are
stable to read from the test thread.

Percent-encoded subjects are decoded before matching, so subjects containing `/` or
spaces behave like they do against a real registry.

## Testing serdes without any server

For deterministic unit tests, seed the cache and skip the network entirely:

```java
var cache = new SchemaCache(new KrabkaSchemaRegistryClient(URI.create("http://127.0.0.1:1")));
cache.seedSubjectId("orders-value", 11);
cache.seedWriterSchema(11, schema.toString());

var serde = AvroSerde.generic(schema, cache, Role.VALUE);
```

The unreachable URI is intentional: if the test ever performs a lookup, it fails
immediately instead of hanging or reaching a real service. This is the pattern
`SchemaSerdesTest` uses for all three formats, including the negative cases: JSON Schema
validation failures and Protobuf `messageType` mismatches.

To test the pending-fetch path, ask for an unseeded ID:

```java
assertThrows(SchemaFetchPendingException.class, () -> cache.writerSchema(7));
```

## Integration tests

The `integrationTest` source set in `krabka-streams-test-utils` runs against live
services. Both tests are annotated with `@EnabledIfEnvironmentVariable`, so they are
skipped unless the corresponding variable is set.

```shell
KRABKA_INTEGRATION_BOOTSTRAP=localhost:9092 \
  ./gradlew :krabka-streams-test-utils:integrationTest

KRABKA_INTEGRATION_SCHEMA_REGISTRY=http://localhost:8081 \
  ./gradlew :krabka-streams-test-utils:integrationTest
```

The task declares both variables as inputs, so changing one invalidates the task and it
re-runs rather than reporting `UP-TO-DATE`.

### BrokerCompatibilityIT

One test that exercises the whole broker-facing surface in a single run. It creates
two-partition input and output topics under a random application ID, starts two
`KafkaStreams` clients configured with `KrabkaStreamsConfig.withDefaults`, exactly-once
v2, one standby replica, and distinct state directories, then:

- waits for both clients to reach `RUNNING` under the streams group protocol;
- produces one record to each partition and asserts the committed output;
- reads the counts back through IQv2 `KeyQuery` with `requireActive()`;
- waits until one of the clients reports a standby task;
- shuts both down, starts a third client on a fresh state directory, and asserts it
  restores the same counts from the changelog.

Requirements on the broker are in [Configuration](configuration.md#broker-requirements).
The standby assertion needs `group.streams.num.standby.replicas=1` on an Apache Kafka
4.3.1 broker; without it the test times out waiting for a standby task.

Every wait is bounded at 90 seconds and reports what it was waiting for, so a failure
names the stage that stalled.

### SchemaRegistryCompatibilityIT

Registers an Avro schema under a random topic, prewarms, round-trips a `GenericRecord`,
and then asserts that `lookup`, `latestId`, and `schemaById` all agree with the ID the
cache resolved, and that the registry stored the Avro canonical parsing form.

### Running the services locally

The CI workflow starts Apache Kafka 4.3.1, krabka 0.3.8, and the krabka schema registry
0.3.8 in containers with the exact flags these tests need. Copy the commands from
[`.github/workflows/integration.yml`](../.github/workflows/integration.yml) rather than
reinventing them; the feature finalization and listener settings are easy to get subtly
wrong.

## Test conventions in this repository

- JUnit 5 (`junit-bom:5.13.4`), configured for every subproject by the root build.
- Parameter matrices use Google's TestParameterInjector JUnit 5 annotations; test
  compilation retains parameter names with `-parameters`.
- Expected and actual objects, records, collections, maps, and arrays use AssertJ's
  recursive comparison so nested mismatches identify their field path.
- Arrow tests wrap allocators and roots in try-with-resources, which turns a leaked
  buffer into a failing test.
- Serde tests seed the cache instead of stubbing HTTP where possible; `RegistryStub`
  (a per-request scripted server, distinct from the shipped `SchemaRegistryStub`) is
  used where the HTTP interaction itself is what is under test.
- Compilation runs with `-Xlint:all -Werror`, so a deprecation in test code fails the
  build. `ColumnarRunnerTest` carries `@SuppressWarnings("deprecation")` because Kafka's
  `MockConsumer(OffsetResetStrategy)` constructor is deprecated.
