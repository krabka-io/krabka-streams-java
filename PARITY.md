# Feature parity

Version `1.0.0` requires each row to have a Java API and a passing test.

| Area | Java implementation | Status |
| --- | --- | --- |
| Streams DSL | Apache Kafka Streams 4.3.1 API dependency | Complete |
| Processor API and punctuators | Apache Kafka Streams 4.3.1 API dependency | Complete |
| State stores and restoration | Apache Kafka Streams 4.3.1 API dependency | Complete |
| Interactive queries and IQv2 | Apache Kafka Streams 4.3.1 API dependency | Complete |
| At-least-once and exactly-once v2 | Apache Kafka Streams 4.3.1 API dependency | Complete |
| Streams group protocol | `KrabkaStreamsConfig` | Complete |
| Schema registry serdes | Native HTTP client, cache, Avro, Protobuf, and JSON Schema serdes | Complete |
| Arrow columnar processing | Arrow IPC, blob and row codecs, operators, topology, and runner | Complete |
| Broker and registry integration tests | `integrationTest` with Apache Kafka 4.3.1 and krabka 0.3.8 images | Complete |

The broker test uses the streams group protocol and exactly-once v2. It also checks local standby
tasks, IQv2 key queries, and state restoration from a changelog. The registry test checks schema
registration, lookup, latest-version reads, ID fetches, Confluent framing, and Avro round trips.
