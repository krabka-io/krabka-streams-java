# Changelog

## 1.4.0 - 2026-08-26

- Add `krabka-streams-coordination`, the Java port of the coordination primitives: leader
  election, leases, and fencing tokens for one role. The leadership epoch is the producer
  epoch that Kafka's transaction coordinator mints for `transactional.id = <role>`, so the
  quorum mints it, the broker enforces it, and a deposed leader does not have to fence
  itself. The lease adds no safety of its own. It decides when a standby stops waiting for
  a quiet holder, and a wrong lease makes a failover early or late rather than making two
  writers authoritative.
- Add `CoordinationCodec`, the codec of the frozen `__coordination_state` record layouts.
  Every integer is big-endian and signed, and a string is an `i16` byte length and then
  plain UTF-8, which is Kafka's own layout and not `DataOutput.writeUTF`. Golden byte
  vectors pin all four layouts, and `krabka-client-rs` and `krabka-streams-go` assert the
  same bytes.
- Compare a fencing token as the pair `(producerId, producerEpoch)`, producer id first.
  The producer epoch is a `short` that wraps, and Kafka then allocates a fresh producer id
  and resets the epoch to zero, so a comparison on the epoch alone accepts a stale writer
  after about 32000 leadership changes. `FencingToken.NO_EPOCH` names a role that no
  member has ever taken.
- Rank candidates on the offset of their registration record, and not on a configuration
  file. `RoleState` folds the partition in offset order, so a recovered node registers
  again, lands at the tail of the roster, and never preempts the member that replaced it.
- Add `Succession.evaluate`, the pure succession rules. They read a role state, an
  instant, and a `LeaseConfig`, and they perform no input and no output, so a test drives
  a whole failover with `ManualClock` and no broker. A challenger of rank `n` challenges
  one stagger later than rank `n - 1`, which saves epoch churn and supplies no safety.
- Pin every record of one role to one partition with `RolePartitioner`, which applies
  Kafka's own key hash to the role name. The registration key and the lease key of one
  role differ, so a key-hashing partitioner would split the role across two partitions and
  destroy the total order that rank depends on.
- Add `KafkaCoordinationTransport` over `Admin.describeTransactions`, a `KafkaProducer`
  with `transactional.id`, a second plain producer for registrations, and a committed-read
  consumer. It maps broker error code 47 `INVALID_PRODUCER_EPOCH` and broker error code 90
  `PRODUCER_FENCED` onto `FencedException`, which is the only signal that a leadership
  ended.
- Add `CoordinationClient`, `Leadership`, and `LeadershipStatus`. `tryAcquire` runs one
  election pass and never blocks, `acquire` repeats that pass until this member wins the
  role, and `describe` answers the third-party question of whether the author of the
  current lease still holds the epoch.

## 1.3.0 - 2026-08-24

- Add `io.krabka.streams.columnar.barrier`, the client side of the broker's barrier
  primitive: `BarrierCutDecoder` decodes the `__barrier_state` records, `BarrierCutReader`
  reads a group's complete cuts over a plain assign, seek, and poll loop, and `BarrierCut`
  carries one epoch's marker offset for every partition. Partial cuts are never returned
  as alignable, because a partition that receives no marker never reaches the cut.
- Align the columnar group runner on a cut. `ColumnarRunner.group` accepts a
  `BarrierAlignment`, truncates each partition at its marker offset, pauses the partition
  until every assigned partition reaches the cut, and then fires the barrier: it snapshots
  each owned partition under the cut's epoch, commits the cut offsets, and calls the
  `BarrierListener`. `runOnceTransactional` does the same work inside the producer
  transaction, so the cut and the transaction boundary coincide.
- Add `GroupRunner.restoreToEpoch` and `GroupRunner.restoreToLatestCut`, which load an
  epoch's snapshot and seek every input partition to the cut.
- Key `ColumnarStateStore` snapshots by partition and epoch. `load` and `save` take an
  epoch, rebalance state uses the new `ColumnarStateStore.LIVE_EPOCH`, and
  `FileColumnarStateStore` writes a cut's state to `partition-<n>-epoch-<e>.snapshot`.
- Change the `FileColumnarStateStore` container to the layout the barrier design freezes
  for all three krabka streams libraries: a big-endian `u32` version, a `u32` entry count,
  and then per entry a `u32` name length, the UTF-8 name, a `u32` value length, and the
  value bytes, with entries in ascending byte order of the name. The previous format wrote
  a 16-bit modified-UTF-8 name length and left entry order to the map, so snapshot files
  from earlier builds do not load.

## 1.2.0 - 2026-08-10

- Add `krabka-streams-columnar-schema`, connecting the schema registry serdes to the
  columnar runtime: `AvroBatchCodec` and `ProtobufBatchCodec` decode registry-framed
  topics into Arrow batches whose columns follow the record schema — structs, lists,
  maps, decimals, and timestamps as native Arrow types — and encode processed batches
  back; `AvroRowBridge`, `ProtobufRowBridge`, `AvroArrowSchemas`, and
  `ProtobufArrowSchemas` expose the conversion for composition.
- Derive every bridge's Arrow schema once, at construction, from the fixed reader
  schema or message descriptor, so mid-stream writer schema evolution never changes
  the columns.
- Add the public `ArrowValues` helpers so custom row bridges and processors reuse the
  engine's type-coercing vector reads and writes, and extend the supported Arrow
  types with `Time`, `FixedSizeBinary`, and exact unsigned 64-bit reads.
- Rethrow retriable failures, such as a pending schema fetch, from the group runner
  regardless of the skip or dead-letter error policy, so transient conditions retry
  instead of discarding healthy batches.
- Syntax-highlight every Javadoc code example, in the module javadoc jars and on the
  documentation site, by running Javadoc with the JDK 25 tool and its
  `--syntax-highlight` option, retinted to the site palette; the Pages site now
  builds through Bazel with a pinned remote JDK, hermetically.

## 1.1.1 - 2026-08-10

- Document every public type, member, and record component with full Javadoc, and add
  a usage example to every public type.
- Enforce complete API documentation by raising the Javadoc lint from
  `Xdoclint:all,-missing` to `Xdoclint:all` with warnings as errors.
- Publish the documentation site — a landing page plus one aggregated, krabka-themed
  Javadoc across every module — to GitHub Pages on each push to `main`.

## 1.1.0 - 2026-08-09

- Resolve the documented columnar, schema registry, serde, and packaging limitations.
- Add record headers, partition-scoped state, snapshots and rebalance hooks, metrics,
  acknowledged asynchronous sends, error policies, and dead-letter output.
- Add event-time windows and joins, JSON-Schema-derived Arrow fields, GZIP codecs, and
  local Avro, JSON Schema, and Protobuf compatibility checks.
- Standardize parameter matrices on TestParameterInjector and structural assertions on
  AssertJ recursive comparison; add deterministic fault injection to the columnar test driver.
- Package strict, example-backed Javadocs before Maven Central uploads and format every
  Java Markdown snippet with the Java language identifier.

## 1.0.0 - 2026-08-08

- Export the Apache Kafka Streams 4.3.1 DSL, Processor API, state stores, and query APIs.
- Enable the KIP-1071 streams group protocol through `KrabkaStreamsConfig`.
- Add native schema registry clients and Avro, Protobuf, and JSON Schema serdes.
- Add Arrow IPC codecs, row and blob modes, vector operations, topologies, and a partition runner.
- Add test utilities and live compatibility tests for Apache Kafka 4.3.1 and krabka 0.3.8.
