# Schema registry

The `krabka-streams-schema-serde` module talks to a Confluent-compatible schema
registry over HTTP and keeps the results in a cache that serdes can read
synchronously. This document covers the client and the cache. The serdes themselves
are in [Serdes](serdes.md).

## Why there is a cache

Kafka's `Serializer` and `Deserializer` interfaces are synchronous. A registry lookup
is not. Blocking inside `serialize` would stall a stream thread on network I/O, and
every implementation that does it has to invent a timeout policy.

krabka splits the two concerns:

- `KrabkaSchemaRegistryClient` is fully asynchronous and returns `CompletableFuture`.
- `SchemaCache` holds resolved IDs and writer schemas in memory.
- Serdes only ever read from the cache. They never perform I/O.

You resolve everything you can before processing starts, with `prewarm`. One case
cannot be predicted: a consumer meeting a schema ID it has never seen. That is handled
with a single background fetch and a retriable exception.

## KrabkaSchemaRegistryClient

```java
var client = new KrabkaSchemaRegistryClient(URI.create("http://localhost:8081"));
```

The injected-client constructor configures TLS, proxies, and timeouts. A convenience
constructor accepts a username and password for HTTP Basic authentication:

```java
var httpClient =
    HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .sslContext(sslContext)
        .build();
var client = new KrabkaSchemaRegistryClient(baseUri, httpClient, new ObjectMapper());
var basic = new KrabkaSchemaRegistryClient(baseUri, username, password);
```

Trailing slashes are normalized and context paths are preserved. Requests set both `Accept` and
`Content-Type` to `application/vnd.schemaregistry.v1+json`. Subjects are URL-encoded,
with `+` rewritten to `%20`, so subjects containing `/` or spaces are safe.

### Operations

| Method                                         | HTTP                                      | Returns                                          |
| ---------------------------------------------- | ----------------------------------------- | ------------------------------------------------ |
| `register(subject, kind, schema, messageType)` | `POST /subjects/{subject}/versions`       | new or existing schema ID                        |
| `lookup(subject, kind, schema, messageType)`   | `POST /subjects/{subject}`                | the ID of an already-registered identical schema |
| `latest(subject)`                              | `GET /subjects/{subject}/versions/latest` | `RegisteredSchema`                               |
| `latestId(subject)`                            | same as `latest`                          | the ID only                                      |
| `schemaById(id)`                               | `GET /schemas/ids/{id}`                   | `FetchedSchema`                                  |
| `subjects()`                                   | `GET /subjects`                           | subject names                                    |
| `versions(subject)`                            | `GET /subjects/{subject}/versions`        | version numbers                                  |
| `version(subject, version)`                    | `GET /subjects/{subject}/versions/{v}`    | `RegisteredSchema`                               |
| `compatibility(...)` / `setCompatibility(...)` | `GET` / `PUT /config[...]`                | compatibility level                              |
| `deleteSubject(...)` / `deleteVersion(...)`    | `DELETE /subjects/...`                    | deleted versions                                 |
| `resolvedSchemaById(id)`                       | ID and referenced-version reads           | schema plus resolved references                  |

Records returned by the client:

```java
public record SchemaReference(String name, String subject, int version) {}

public record RegisteredSchema(..., List<SchemaReference> references) {}

public record FetchedSchema(String schema, String messageType, List<SchemaReference> references) {}
```

`schemaType` and `messageType` are `null` when the registry omits them. Avro is the
registry default and is sent without a `schemaType` field at all; `PROTOBUF` and
`JSON` are sent explicitly. `messageType` carries the fully qualified Protobuf message
name and is omitted for the other formats.

### Errors

Every failure path produces a `SchemaRegistryException`, wrapped in a
`CompletionException` when it surfaces from `join()`:

| Situation                         | `statusCode()`  | Message                                                              |
| --------------------------------- | --------------- | -------------------------------------------------------------------- |
| Non-2xx response                  | the HTTP status | `schema registry returned HTTP 404: ...` including the response body |
| Transport failure                 | `-1`            | `schema registry request failed` with the cause attached             |
| Unparseable response              | `-1`            | `cannot parse schema registry response`                              |
| Response missing a required field | `-1`            | `schema registry response has no integer id`                         |

```java
try {
  client.schemaById(7).join();
} catch (CompletionException error) {
  if (error.getCause() instanceof SchemaRegistryException registry
      && registry.statusCode() == 404) {
    // the ID does not exist
  }
}
```

The client retries transport failures, HTTP 429, and 5xx responses twice by default.
The four-argument injected-client constructor configures that retry count.

## SchemaCache

```java
var cache = new SchemaCache(client);
var strict = new SchemaCache(client, RegisterMode.LOOKUP_ONLY, new TopicNameStrategy());
```

The single-argument constructor uses `RegisterMode.AUTO_REGISTER` and
`TopicNameStrategy`. All internal maps are `ConcurrentHashMap`, so a cache is safe to
share across stream threads, and normally you want exactly one per application so that
its contents are shared.

### Register modes

| Mode            | Registry call during `prewarm`            | Use when                                                    |
| --------------- | ----------------------------------------- | ----------------------------------------------------------- |
| `AUTO_REGISTER` | `POST /subjects/{subject}/versions`       | Development, or producers that own the schema               |
| `LOOKUP_ONLY`   | `POST /subjects/{subject}`                | Production, where an unregistered schema must fail          |
| `USE_LATEST`    | `GET /subjects/{subject}/versions/latest` | Consumers that follow whatever the registry currently holds |

`USE_LATEST` is the only mode that adopts the registry's `messageType` in place of the
locally derived one; the other two keep the local value.

### Subject naming

```java
@FunctionalInterface
public interface SubjectNameStrategy {
  String subject(String topic, Role role);
}
```

`TopicNameStrategy` implements the Confluent rule: `orders` becomes `orders-key` or
`orders-value` depending on `Role`. Supply your own strategy for record-name or
topic-record-name conventions:

```java
SubjectNameStrategy recordName = (topic, role) -> "com.example.Order";
var serde = AvroSerde.forValue(Order.class, cache, recordName);
```

Each serde factory accepts an optional strategy, so one cache can serve topic-name and
record-name subjects together. `cache.subject(topic, role, strategy)` exposes the same
calculation.

### The prewarm cycle

```java
serde.registerSubject("orders"); // interns the subject; no I/O
otherSerde.registerSubject("payments"); // idempotent per subject
cache.prewarm().join(); // one registry call per interned subject
```

`registerSubject` calls `cache.intern(subject, kind, schema, messageType)`, which uses
`putIfAbsent`. Interning the same subject twice keeps the first schema. `prewarm`
issues one request per interned subject in parallel and completes when all of them
complete; a single failure fails the returned future.

Use `prewarmReport()` when startup should continue after partial success. Its
`PrewarmReport` contains independent `resolved` and `failures` maps.

After `prewarm` the cache holds, for each subject:

- `subjectIds[subject] = id`, which serializers read through `idForSubject`.
- `writerSchemas[id] = schema`, the local schema text, or the registry's text under
  `USE_LATEST`.
- `writerMessageTypes[id] = messageType`, when one exists.
- `writerReferences[id] = schemas`, resolved recursively by reference name.

You can call `prewarm` again later, for example after adding a serde at runtime.
Subjects that are already resolved are simply resolved again.

### Reading writer schemas

```java
public String writerSchema(int schemaId); // throws SchemaFetchPendingException on a miss

public String writerMessageType(int schemaId); // null when unknown

public OptionalInt idForSubject(String subject);
```

`writerSchema` behaves differently from the other two. On a hit it returns
immediately. On a miss it starts exactly one background `GET /schemas/ids/{id}` and
throws `SchemaFetchPendingException`. Concurrent callers for the same ID join the
in-flight fetch rather than starting another.

That exception extends Kafka's `RetriableException`, so a Kafka Streams deserialization
error handler can return `CONTINUE`/retry semantics, and a plain consumer loop can
retry the record. By the time the retry arrives, the fetch has usually completed.

```java
while (true) {
  try {
    return serde.deserializer().deserialize(topic, bytes);
  } catch (SchemaFetchPendingException pending) {
    Thread.sleep(10); // the fetch for pending.schemaId() is already running
  }
}
```

If the background fetch fails, the marker is removed and the next call starts a fresh
fetch, so a transient registry outage recovers without extra bookkeeping.

### Seeding

Three methods write to the cache directly and perform no I/O:

```java
cache.seedSubjectId("orders-value", 11);
cache.seedWriterSchema(11, schemaText);
cache.seedWriterMessageType(11, "demo.Order");
```

They exist for deterministic tests and for offline or air-gapped startup, where schema
IDs are pinned by configuration instead of discovered. A cache that is fully seeded
never contacts the registry, and the client instance it holds is never used.

## Full example

```java
var client = new KrabkaSchemaRegistryClient(URI.create("http://localhost:8081"));
var cache = new SchemaCache(client, RegisterMode.LOOKUP_ONLY, new TopicNameStrategy());

var orderSerde = AvroSerde.forValue(Order.class, cache);
var keySerde = AvroSerde.forKey(OrderKey.class, cache);
orderSerde.registerSubject("orders");
keySerde.registerSubject("orders");

cache.prewarm().join(); // fails fast if either subject is unregistered

var builder = new StreamsBuilder();
builder.stream("orders", Consumed.with(keySerde, orderSerde))
    .to("orders-copy", Produced.with(keySerde, orderSerde));
```

Calling `prewarm().join()` before `streams.start()` turns a registry problem into a
startup failure with a clear message, instead of a serialization failure on the first
record.
