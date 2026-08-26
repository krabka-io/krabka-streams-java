# Coordination

One role elects one leader, and every guarded write carries the proof of that
leadership. A control plane that dispatches commands to external devices needs two
properties from its log. A command is durable in a quorum before the dispatch, and a
deposed leader cannot dispatch at all. The second property is the hard one. A leader
that loses its lease does not know that it lost the lease, so it cannot fence itself.
Something outside the leader must refuse its writes.

Everything in this page lives in `io.krabka.streams.coordination`, inside the
`krabka-streams-coordination` artifact.

## The epoch is the safety mechanism and the lease is not

The leadership epoch is the producer epoch that Kafka's transaction coordinator mints
for `transactional.id = <role>`. That one decision supplies every property this module
promises:

- The coordinator writes the epoch to `__transaction_state`, which is replicated, so
  the quorum mints the epoch.
- The coordinator advances the epoch on every `InitProducerId` call and reuses no
  value, so the epoch grows.
- The broker rejects a write that carries a superseded epoch. It answers
  `INVALID_PRODUCER_EPOCH` or `PRODUCER_FENCED`. The cluster fences a deposed leader,
  and the leader does not fence itself.
- `DescribeTransactions` reports the current producer id and producer epoch of a
  transactional id. Any process calls it, so a third party checks the authority of a
  writer with one request and no membership.

The lease adds no safety of its own. It is a liveness and anti-flap device. It tells a
standby when to stop waiting for a quiet holder. A wrong lease makes a failover early or
late. A wrong lease never makes two writers authoritative.

Read that inversion against a hand-rolled design, where the lease deadline decides who
may write. This module never asks the lease that question. A leader does not have to
prove that its lease is live before each write. The write itself carries the proof, and
the broker checks it.

## Taking a role

```java
Role role = Role.of("controller");
MemberId me = MemberId.of("node-1");

try (CoordinationClient client = new CoordinationClient(transport);
    Leadership leadership = client.acquire(role, me, Duration.ofMinutes(1))) {
  while (running) {
    if (leadership.renewDue()) {
      leadership.renew();
    }
    dispatch(leadership.token());
  }
} catch (FencedException lost) {
  controller.stop();
}
```

`acquire` registers this member, reads the coordination topic, and repeats one election
pass until the member wins the role or the deadline passes. `tryAcquire` runs one pass
and never blocks, so a caller that drives its own loop uses that instead.

`FencedException` is the only signal that a leadership ended. A caller stops the work of
the role at once and does not retry. The epoch it held is superseded, so the cluster
already rejects every write that the member makes.

## The fencing token is a pair

The producer epoch is a `short` and it wraps. Kafka handles the exhaustion with a fresh
producer id and an epoch of zero. An implementation that compares the epoch alone
accepts a stale writer after about 32000 leadership changes.

`FencingToken` holds the pair `(producerId, producerEpoch)` and compares the producer id
first. Kafka allocates producer ids from a monotonic block allocator, so the pair stays
monotonic where the epoch alone does not.

```java
FencingToken wrapped = FencingToken.of(4, Short.MAX_VALUE);
FencingToken fresh = FencingToken.of(5, (short) 0);
assert fresh.supersedes(wrapped);
```

`FencingToken.NO_EPOCH` is the token of a role that no member has ever taken. It ranks
below every minted token, and `FencingToken.of` rejects a negative value, so no minted
token can equal it.

## Rank comes from the log, not from configuration

A candidate appends a registration record to `__coordination_state`. The offset of that
record in the partition is its join sequence. Log compaction keeps the offset of every
record it retains, so a reader that walks the partition in offset order sees the
registrations in registration order. `RoleState` folds that walk, and the roster it
builds is in offset order.

A recovered node registers again. The new record takes a higher offset, so the node
lands at the tail of the roster and takes the last rank. That is the no-failback rule,
and it needs no counter and no coordinator. A rank from a configuration file would put
the recovered node at the front, and the node would then preempt the member that
replaced it. The cluster would pay for a second failover that it does not need.

## The stagger is an optimisation

A challenger of rank `n` challenges at `deadline + n * challengeStagger`. The live
standby closest to the front wins, and the standbys behind it never enter the race.

The stagger saves epoch churn, and it does nothing else. `InitProducerId` is atomic at
the coordinator, so a simultaneous challenge by every standby still leaves exactly one
member with the newest epoch. The losers keep an older epoch, and each one learns that
it lost on its first guarded write. Set the stagger to make that outcome rare. Do not
set it to make the outcome safe, because the outcome is already safe.

`Succession.evaluate` applies the rules and returns a `Decision`. The rules are pure, so
a test drives a whole failover with `ManualClock` and no broker.

| Action           | Meaning                                                        |
| ---------------- | -------------------------------------------------------------- |
| `NOT_REGISTERED` | This member has no rank yet. It registers and evaluates again. |
| `HOLD`           | This member holds the role and its lease is live. It renews.   |
| `CHALLENGE`      | This member calls `InitProducerId` now.                        |
| `WAIT`           | This member waits until `Decision.waitUntilMillis`.            |

The anchor of the challenge instant always comes from the role state, and never from the
current instant. A member that has no lease to wait for anchors on its own registration
record. An anchor of "now plus `n` staggers" would move forward on every evaluation, and
a standby of rank 1 or more would then wait for ever while rank 0 is dead.

## The frozen record format

Per-role state lives in the compacted internal topic `__coordination_state`. The topic
carries two record kinds and the key discriminates them. Every integer is big-endian and
signed. A string is an `i16` byte length and then plain UTF-8 bytes. This is Kafka's own
native string layout, and it is the layout `__barrier_state` uses. It is not Java's
modified UTF-8, which `DataOutput.writeUTF` produces.

```text
key:
  version  i16 = 0
  kind     i16          0 registration, 1 lease
  role     string
  member   string       the member id for kind 0, the empty string for kind 1

registration value:
  version        i16 = 0
  member         string
  registered_at  i64    epoch milliseconds

lease value:
  version         i16 = 0
  member          string
  producer_id     i64
  producer_epoch  i16
  granted_at      i64   epoch milliseconds
  deadline        i64   epoch milliseconds
```

A record with a null value is a tombstone. A tombstone of a registration key
deregisters one member, and a tombstone of a lease key clears the lease of one role.

The layouts are frozen. `krabka-client-rs` and `krabka-streams-go` implement them byte
for byte, and all three projects assert the same golden vectors. A change to one
implementation that the others do not follow fails a test rather than reaching a
cluster.

## All records of one role go to one partition

The succession rules rank candidates on the offset of their registration, so the records
of one role need a total order, and one partition gives that order. Both writers of a
role pin the partition with `RolePartitioner.partitionFor`, which is Kafka's own key
partitioning: `murmur2` of the role name in UTF-8, masked with `Utils.toPositive`, and
then the remainder of the partition count.

The pin is a correctness requirement and not a preference. Kafka's default partitioner
hashes the record key. The registration key of a role and the lease key of the same role
differ, because the registration key names a member and the lease key does not. A
partitioner that reads the key puts the two kinds in two partitions, and the total order
is gone.

## The transport

`CoordinationTransport` holds every cluster operation this module performs, and nothing
else. `KafkaCoordinationTransport` is the implementation that talks to a cluster.

```java
try (Admin admin = Admin.create(adminSettings);
    Producer<byte[], byte[]> registrar = new KafkaProducer<>(plainSettings);
    Consumer<byte[], byte[]> reader = new KafkaConsumer<>(committedReadSettings);
    KafkaCoordinationTransport transport = new KafkaCoordinationTransport(
        admin, registrar, role -> new KafkaProducer<>(transactionalSettings(role)), reader)) {
  transport.register(role, me, System.currentTimeMillis());
}
```

A role needs two producers. The transactional producer carries
`transactional.id = <role>`, and it writes the lease records of that role. A second,
plain producer appends the registration records, because Kafka puts every send of a
transactional producer inside a transaction, and a candidate holds no epoch to open one
with.

A Kafka producer calls `initTransactions` once, and the epoch it receives binds to that
instance for its whole life. A second `acquireEpoch` for one role builds a second
producer and closes the first.

Configure the reading consumer with `isolation.level=read_committed`, so an aborted
lease write stays invisible.

## Topic and broker requirements

| Setting               | Value         | Why                                           |
| --------------------- | ------------- | --------------------------------------------- |
| `cleanup.policy`      | `compact`     | The topic keeps the last record of every key  |
| `min.insync.replicas` | 2 or more     | "Durable in a quorum" rests on this           |
| `acks`                | `all`         | The client sets it; the topic config gates it |
| Partitions            | 16 by default | `RolePartitioner.DEFAULT_PARTITIONS`          |

The claim "durable in a quorum before the dispatch" rests on the topic configuration and
not on this client.

## Checking a writer from outside

`CoordinationClient.describe` asks the transaction coordinator for the token of a role
and folds the coordination topic. The call takes no epoch and joins no group.

```java
LeadershipStatus status = client.describe(role);
status.holder();  // the member the last lease record names
status.token();   // the token the coordinator reports
status.current(); // whether that member still holds the epoch
```

`current()` is the third-party check. A false answer means one of two things. Either no
member holds the role, or a challenger minted a newer epoch and has not written its own
lease yet. In both cases the broker already rejects every write that the author of the
lease makes.

## Clock assumptions

The lease deadline is a wall-clock instant. The holder writes it, and a challenger
compares it against its own clock. Skew between the two clocks moves the failover time
by the size of the skew. Skew does not affect safety, for the reason the first section
gives. An operator that needs a bound on the failover time needs a bound on the skew.

`LeaseConfig` holds the three timings. The defaults are a 30-second lease, a 10-second
renew interval, and a 5-second challenge stagger. Set the renew interval to at most a
third of the duration, so the holder keeps two more attempts before the deadline.
`LeaseConfig.renewsWithMargin` reports which side of that bound a value is on.

## Types

| Type                         | Purpose                                                    |
| ---------------------------- | ---------------------------------------------------------- |
| `Role`, `MemberId`           | Validated names; a role becomes a `transactional.id`       |
| `FencingToken`               | The quorum-minted pair, compared producer id first         |
| `Registration`, `Lease`      | The two record values                                      |
| `CoordinationKey`            | The decoded key: kind, role, and member                    |
| `CoordinationCodec`          | The frozen codec; `TOPIC` names the topic                  |
| `CoordinationEntry`          | One decoded record with the offset it sits at              |
| `RolePartitioner`            | The partition of a role                                    |
| `RoleState`, `RosterEntry`   | The folded roster and lease of one role                    |
| `RoleStateBuilder`           | Folds records one at a time                                |
| `Succession`, `Decision`     | The pure succession rules and their answer                 |
| `LeaseConfig`, `LeaseTiming` | The lease policy and the lease clock                       |
| `Clock`, `ManualClock`       | The clock seam and the clock a test drives                 |
| `CoordinationTransport`      | The cluster seam                                           |
| `KafkaCoordinationTransport` | The implementation over `Admin`, producers, and a consumer |
| `CoordinationClient`         | The election loop                                          |
| `Leadership`                 | The live handle: token, renew, resign                      |
| `LeadershipStatus`           | The third-party view of one role                           |
| `CoordinationException`      | Every failure of this module                               |
| `FencedException`            | The loss of a role, and the only such signal               |
