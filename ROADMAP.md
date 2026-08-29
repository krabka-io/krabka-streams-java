# Roadmap

This roadmap lists the work planned after `1.4.0`. It is a statement of intent, not a
commitment to dates: the milestone dates are targets that move when the work does.

The roadmap has one machine-readable source, [`.github/roadmap.yml`](.github/roadmap.yml).
The [Roadmap workflow](.github/workflows/roadmap.yml) reads that file and creates or
updates the matching GitHub labels, milestones, and issues. Editing the file is how the
roadmap changes; the issues follow.

## Principles

These hold for every item below, and an item that violates one is the wrong item.

- **The Kafka Streams API stays re-exported, not wrapped.** New surface goes beside it,
  never in front of it. See [Architecture](docs/architecture.md).
- **A wire layout shared with `krabka-client-rs` and `krabka-streams-go` is frozen.**
  Changing one means changing three libraries and their golden vectors together.
- **A parity row needs a Java API and a passing test**, which is the bar
  [PARITY.md](PARITY.md) already applies to every shipped area.
- **The columnar runtime owns its own semantics.** It is a separate runtime, so its
  operators, state, and barriers are designed as a whole rather than mirrored from the
  Kafka Streams DSL.

## Milestones

### 1.5.0 — Operability

Target: 2026-10-15. Running the columnar runtime and the coordination client in production, and being
able to see what they are doing.

| Item                                                             | Area         |
| ---------------------------------------------------------------- | ------------ |
| Export columnar metrics through Kafka's metrics registry and JMX | Columnar     |
| Add tracing hooks around columnar batch processing               | Columnar     |
| Retain and reclaim epoch-keyed columnar snapshots                | Columnar     |
| Report why a barrier cut has not aligned                         | Columnar     |
| Inspect every role's roster and lease                            | Coordination |

### 1.6.0 — Columnar depth

Target: 2026-12-15. The gaps between the columnar operator set and the topologies people actually
write.

| Item                                         | Area     |
| -------------------------------------------- | -------- |
| Add sort, top-k, and distinct operators      | Columnar |
| Add session and sliding windows              | Columnar |
| Allow user-defined aggregate functions       | Columnar |
| Render a built topology as an execution plan | Columnar |
| Add a spillable columnar state store         | Columnar |

### 1.7.0 — Schema registry depth

Target: 2027-02-15. The registry client covers the endpoints; this milestone covers the deployments.

| Item                                                               | Area            |
| ------------------------------------------------------------------ | --------------- |
| Add record-name and topic-record-name subject strategies           | Schema registry |
| Authenticate to the registry with a bearer token or mutual TLS     | Schema registry |
| Make registry timeouts, backoff, and negative caching configurable | Schema registry |
| Bound the schema cache                                             | Schema registry |
| Resolve JSON Schema `$ref` through registry references             | Schema registry |

### 2.0.0 — Platform and packaging

Target: 2027-04-15. The changes that need a major version because they move the floor under consumers.

| Item                                                 | Area  |
| ---------------------------------------------------- | ----- |
| Publish real JPMS module descriptors                 | Build |
| Move to Apache Arrow 20 and drop `--add-opens`       | Build |
| Support Java 25 across the build and test matrix     | Build |
| Document and automate the Kafka version upgrade path | Build |

### Engineering quality

Continuous, with no target date. Work that makes every other milestone cheaper.

| Item                                                                      | Area         |
| ------------------------------------------------------------------------- | ------------ |
| Add a JMH benchmark module for codecs and operators                       | Build        |
| Run Error Prone and NullAway in the Bazel build                           | Build        |
| Add CodeQL and OpenSSF Scorecard workflows                                | Build        |
| Test leader failover and fencing against a live broker                    | Coordination |
| Share conformance vectors with `krabka-client-rs` and `krabka-streams-go` | Build        |

## Changing the roadmap

1. Edit [`.github/roadmap.yml`](.github/roadmap.yml). Every issue carries an `id` that
   is its permanent identity; the sync matches on that `id`, so a title or body may be
   rewritten freely, and a changed `id` creates a second issue.
2. Update the milestone tables above. The sync checks that this file names every
   milestone and every issue title in `.github/roadmap.yml`, and fails when it does not.
3. Open a pull request. The Roadmap workflow runs a dry run on the pull request and
   prints the milestones and issues it would create or update.
4. Merge. The workflow runs for real on `main` and applies the plan.

Removing an item from the file does not close its issue. Close it on GitHub, with a
reason, and delete the entry in the same pull request.
