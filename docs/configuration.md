# Configuration

## KrabkaStreamsConfig

`io.krabka.streams.KrabkaStreamsConfig` is the only type in the `krabka-streams`
module. It is a final utility class with one static method.

```java
public static Properties withDefaults(Map<?, ?> settings)
```

The method copies every entry of `settings` into a fresh `Properties` object and then
applies krabka defaults with `putIfAbsent`. The input map is never modified, and the
returned object shares no state with it.

Constants:

| Constant | Value |
| --- | --- |
| `KrabkaStreamsConfig.GROUP_PROTOCOL_CONFIG` | `"group.protocol"` |
| `KrabkaStreamsConfig.STREAMS_GROUP_PROTOCOL` | `"streams"` |

Passing `null` throws `NullPointerException` with the message `settings`.

### The default it applies

`group.protocol=streams` selects the KIP-1071 streams group protocol, in which the
broker owns task assignment instead of the client. This is the protocol krabka brokers
implement, and it is the reason the helper exists.

An explicit setting always wins:

```java
var classic = KrabkaStreamsConfig.withDefaults(Map.of("group.protocol", "classic"));
// classic.get("group.protocol") == "classic"
```

That escape hatch matters when you run against a broker that has not finalized
`streams.version=1`, or when you are comparing behavior between protocols.

### Typical use

```java
var settings = new HashMap<String, Object>();
settings.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
settings.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
settings.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
settings.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString());
settings.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "host:18081");
settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

var streams = new KafkaStreams(topology, KrabkaStreamsConfig.withDefaults(settings));
```

Because the result is a plain `Properties`, you can still adjust it afterwards:

```java
var properties = KrabkaStreamsConfig.withDefaults(settings);
properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
```

Every other Kafka Streams setting is passed through untouched. `krabka-streams` adds
no configuration namespace of its own; use `StreamsConfig`, `ConsumerConfig`,
`ProducerConfig`, and `AdminClientConfig` keys as you normally would.

## Broker requirements

The streams group protocol needs both of the following on the broker:

1. The `streams` rebalance protocol enabled in the group coordinator.
2. The `streams.version=1` feature finalized on the cluster.

For Apache Kafka 4.3.1 in a container that means, at minimum:

```text
KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer,streams
KAFKA_UNSTABLE_API_VERSIONS_ENABLE=true
KAFKA_UNSTABLE_FEATURE_VERSIONS_ENABLE=true
```

followed by finalizing the feature:

```shell
kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature streams.version=1
```

To exercise standby tasks against Apache Kafka 4.3.1, also set
`group.streams.num.standby.replicas=1` (`KAFKA_GROUP_STREAMS_NUM_STANDBY_REPLICAS=1`).
Under the streams protocol the broker, not the client, decides how many standby
replicas exist, so the client-side `num.standby.replicas` alone is not enough.

A krabka broker finalizes the feature at format time:

```shell
crabka format --log-dir /tmp/crabka-data --standalone --node-id 1 \
  --cluster-id 00000000-0000-0000-0000-000000000001 \
  --controller-listener 127.0.0.1:9093 \
  --feature streams.version=1
```

The exact container invocations that CI uses are in
[`.github/workflows/integration.yml`](../.github/workflows/integration.yml).

## JVM flags

Arrow 19 accesses `java.nio` internals when it allocates direct buffers. Any JVM that
runs columnar code needs the following, whether it is an application, a test, or a
command-line tool:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

This repository sets the flag for the `krabka-streams-columnar` and
`krabka-streams-test-utils` test tasks, and for the `integrationTest` task. In your own
build, add it wherever Arrow runs:

```kotlin
tasks.withType<Test>().configureEach {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}

tasks.withType<JavaExec>().configureEach {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}
```

For a packaged application, put it on the command line or in `JAVA_TOOL_OPTIONS`.
Omitting it surfaces as an `InaccessibleObjectException` or a `RuntimeException` from
Arrow's memory subsystem the first time a batch is allocated. Modules that never touch
Arrow (`krabka-streams`, `krabka-streams-schema-serde`) do not need the flag.

You may also want to bound Arrow's off-heap use with `-XX:MaxDirectMemorySize`, since
Arrow allocations do not count against the Java heap.

## Environment variables

These affect the build and tests only, never the library at runtime.

| Variable | Used by | Effect |
| --- | --- | --- |
| `KRABKA_INTEGRATION_BOOTSTRAP` | `BrokerCompatibilityIT` | Broker address; the test is skipped when unset |
| `KRABKA_INTEGRATION_SCHEMA_REGISTRY` | `SchemaRegistryCompatibilityIT` | Registry base URI; the test is skipped when unset |
| `MAVEN_CENTRAL_USERNAME` | `publish*` tasks | Central Portal user token |
| `MAVEN_CENTRAL_PASSWORD` | `publish*` tasks | Central Portal password token |
| `SIGNING_KEY` | `signing` plugin | In-memory ASCII-armored PGP key; signing is skipped when blank |
| `SIGNING_PASSWORD` | `signing` plugin | Passphrase for that key |

Both integration tests are annotated with `@EnabledIfEnvironmentVariable`, so running
`integrationTest` without the variables set reports zero failures and zero executed
tests rather than an error.
