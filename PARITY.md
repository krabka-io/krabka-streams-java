# Feature parity

Version `1.4.0` requires each row to have a Java API and a passing test.

| Area                                  | Java implementation                                                                        | Status   |
| ------------------------------------- | ------------------------------------------------------------------------------------------ | -------- |
| Streams DSL                           | Apache Kafka Streams 4.3.1 API dependency                                                  | Complete |
| Processor API and punctuators         | Apache Kafka Streams 4.3.1 API dependency                                                  | Complete |
| State stores and restoration          | Apache Kafka Streams 4.3.1 API dependency                                                  | Complete |
| Interactive queries and IQv2          | Apache Kafka Streams 4.3.1 API dependency                                                  | Complete |
| At-least-once and exactly-once v2     | Apache Kafka Streams 4.3.1 API dependency                                                  | Complete |
| Streams group protocol                | `KrabkaStreamsConfig`                                                                      | Complete |
| Schema registry serdes                | Native HTTP client, cache, Avro, Protobuf, and JSON Schema serdes                          | Complete |
| Arrow columnar processing             | Arrow IPC, blob and row codecs, operators, topology, and runner                            | Complete |
| Avro and Protobuf columnar bridges    | `AvroBatchCodec`, `ProtobufBatchCodec`, row bridges, converters                            | Complete |
| Barrier-aligned state snapshots       | `BarrierCutReader`, `BarrierAlignment`, `BarrierCut`, and epoch-keyed `ColumnarStateStore` | Complete |
| Leader election, leases, fencing      | `CoordinationClient`, `Leadership`, `FencingToken`, `Succession`, and `CoordinationCodec`  | Complete |
| Broker and registry integration tests | `integrationTest` with Apache Kafka 4.3.1 and krabka 0.3.8 images                          | Complete |

The broker test uses the streams group protocol and exactly-once v2. It also checks local standby
tasks, IQv2 key queries, and state restoration from a changelog. The registry test checks schema
registration, lookup, latest-version reads, ID fetches, Confluent framing, and Avro round trips.

The barrier row covers the client side of the broker's barrier primitive. A coordinator injects
epoch-stamped markers and publishes each cut to `__barrier_state`; the columnar group runner reads
those cuts, aligns every partition on one, snapshots the state under the epoch, and restores back
to it. See [Barrier alignment](docs/barriers.md).

The coordination row covers leader election, leases, and fencing tokens. The leadership epoch is
the producer epoch that Kafka's transaction coordinator mints for `transactional.id = <role>`, so
the broker fences a deposed leader and the lease decides only when a standby challenges. The
`__coordination_state` record layouts and the role-to-partition rule are frozen, and the tests
assert the same golden vectors as `krabka-client-rs` and `krabka-streams-go`.
See [Coordination](docs/coordination.md).
