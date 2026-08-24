# Barrier alignment

A krabka broker can put an exact, reproducible cut across a set of topics. A
coordinator injects an epoch-stamped marker into every partition of a named barrier
group and publishes the resulting cut to the internal topic `__barrier_state`. The cut
is a marker offset for each partition. Records below that offset are before the cut and
records at or above it are after the cut.

The columnar group runner uses that cut for three things: it aligns processing on one
exact point in every input, it snapshots operator state there under the cut's epoch, and
it restores back to that point later.

Everything in this page lives in `io.krabka.streams.columnar.barrier`, inside the
`krabka-streams-columnar` artifact.

## Why the runner reads a topic instead of the markers

A barrier marker is a Kafka control record. The JVM `KafkaConsumer` drops every control
batch before the application sees it, so a Java client can never observe a marker in
band. The consumer still moves its position past the marker's offset, so no offset
arithmetic breaks.

That is why the coordinator publishes a cut manifest to `__barrier_state`. Java reads
the manifest and compares its consumed offsets against it. Do not try to observe the
markers.

## Reading cuts

`BarrierCutReader` drives a consumer of your own with `assign`, `seek`, and `poll`. It
needs no consumer group and no new broker RPC.

```java
var cutConsumer = new KafkaConsumer<byte[], byte[]>(settings);
var reader = new BarrierCutReader(cutConsumer);

Optional<BarrierCut> latest = reader.latestCompleteCut("transactions");
List<BarrierCut> newer = reader.completeCutsAfter("transactions", 41);
```

Give the reader a consumer of its own. The first read replaces that consumer's
assignment with every partition of `__barrier_state` and seeks to the beginning. Later
reads continue from where the last read stopped, so a read costs one poll of what the
coordinator published since.

The reader returns complete cuts only. A partial cut names partitions that never
received the epoch's marker, so a task that waited for one would wait forever. The
coordinator publishes partial cuts for exactly this reason: a reader can skip the epoch
and does not stall.

| Type                | Purpose                                                            |
| ------------------- | ------------------------------------------------------------------ |
| `BarrierCut`        | One epoch's cut: group, epoch, times, status, offsets, and missing |
| `BarrierCutStatus`  | `COMPLETE` or `PARTIAL`                                            |
| `BarrierCutDecoder` | Decodes one `__barrier_state` record; `TOPIC` names the topic      |
| `BarrierCutReader`  | Reads a group's complete cuts from that topic                      |
| `BarrierAlignment`  | The runner option: group, reader, and listener                     |
| `BarrierListener`   | The callback that fires when a barrier is reached                  |

`BarrierCut` also carries the alignment arithmetic:

| Method                                | Result                                                   |
| ------------------------------------- | -------------------------------------------------------- |
| `offset(TopicPartition)`              | The partition's marker offset, or empty                  |
| `recordsBefore(TopicPartition, List)` | The records whose offset is below the marker offset      |
| `reached(TopicPartition, long)`       | True when a position delivered everything below the cut  |
| `complete()`                          | True when every partition of the group received a marker |

A partition that the cut does not name is not in the barrier group. It holds nothing
back and never delays the barrier.

## Aligning a group runner

Pass a `BarrierAlignment` to the `ColumnarRunner.group` overload that takes one.

```java
var alignment = BarrierAlignment.on("transactions", new BarrierCutReader(cutConsumer))
    .withListener(cut -> log.info("aligned on epoch {}", cut.epoch()));

try (var runner = ColumnarRunner.group(
    topology, consumer, producer,
    ColumnarErrorPolicy.fail(),
    new FileColumnarStateStore(stateDirectory),
    new ColumnarMetrics(),
    alignment)) {
  while (running) {
    runner.runOnce(Duration.ofSeconds(1));
  }
}
```

Each cycle works like this.

1. The runner takes the lowest complete cut above the last epoch it fired. A cut whose
   offsets are already behind the consumer's position cannot be aligned on, so the
   runner skips that epoch.
2. Every polled partition is truncated at its marker offset. Records with
   `offset < cutOffset` are processed. The rest wait.
3. A truncated partition commits at the marker offset, seeks back to it, and pauses. It
   stays paused until the barrier fires, so it does not re-fetch the same records every
   cycle.
4. When every assigned partition reaches its marker offset, the barrier fires.

A barrier that fires does three things in order. It snapshots each owned logical
partition with `snapshotPartition`, it saves the snapshot under the cut's epoch, and it
commits the cut offsets. The stored state is then exactly the committed state at the
cut. The listener runs last, on the thread that drives the runner.

`runOnceTransactional` puts the same work inside the producer transaction. The output
records, the consumed offsets, and the snapshot all become durable together, so the cut
and the transaction boundary coincide. A failure aborts the transaction and rolls the
partition state back.

## Epoch-keyed snapshots

`ColumnarStateStore` keys every snapshot by partition and epoch.

```java
Map<String, byte[]> load(int partition, long epoch);

void save(int partition, long epoch, Map<String, byte[]> snapshot);
```

Rebalance saves and loads use `ColumnarStateStore.LIVE_EPOCH`, which is `-1`. A barrier
epoch is never negative, so the running state and a cut's state never collide.
`FileColumnarStateStore` writes the running state to `partition-<n>.snapshot` and a
cut's state to `partition-<n>-epoch-<e>.snapshot`.

The file container is the layout the barrier design freezes for all three krabka streams
libraries.

```text
version u32 = 1
count   u32
entries count x [ name_len u32 | name UTF-8 | value_len u32 | value bytes ]
```

Every integer is big-endian and entries are in ascending byte order of the name, so the
same snapshot always produces the same bytes. The payload inside each entry stays
language-specific: a Java snapshot does not restore into a Go or Rust task.

## Restoring to a cut

```java
BarrierCut restored = runner.restoreToEpoch(41);
Optional<BarrierCut> latest = runner.restoreToLatestCut();
```

A restore drops the operator state of every owned logical partition, loads the epoch's
snapshot, and seeks every assigned partition that the cut names to its marker offset.
Processing then continues from the cut. Call it between cycles, not inside one.

`restoreToEpoch` raises `ColumnarException` when the group has no complete cut for the
epoch. Both methods raise `ColumnarException` when the runner has no alignment.

## What the cut does and does not promise

A cut is exact and reproducible. Two replays of epoch N read the same records, and the
cut survives restart, failover, and compaction.

A cut is not causally consistent across independent producers. A producer can write to
topic A after epoch N's marker lands in A and then write to topic B before the marker
lands in B, and the cut inverts that producer's own order. Barrier alignment is right
for checkpoints, replay points, audit snapshots, and shadow runs. It cannot answer
whether one producer's write to A happened before its write to B.

## Next

- [Columnar processing](columnar.md) for the batch model and the runner
- [Architecture](architecture.md) for the design decisions
- [API reference](api-reference.md) for every public type
