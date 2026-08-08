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
| Broker and registry integration tests | `integrationTest` | In progress |
