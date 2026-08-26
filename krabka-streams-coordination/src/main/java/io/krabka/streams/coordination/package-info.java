/**
 * Leader election, leases, and fencing tokens for one role.
 *
 * <p>A control plane that dispatches commands to external devices needs two properties
 * from its log. A command is durable in a quorum before the dispatch, and a deposed
 * leader cannot dispatch at all. The second property is the hard one. A leader that
 * loses its lease does not know that it lost the lease, so it cannot fence itself.
 * Something outside the leader must refuse its writes.
 *
 * <h2>The epoch is the safety mechanism and the lease is not</h2>
 *
 * <p>The leadership epoch is the producer epoch that Kafka's transaction coordinator
 * mints for {@code transactional.id = <role>}. That one decision supplies every
 * property this package promises:
 *
 * <ul>
 *   <li>The coordinator writes the epoch to {@code __transaction_state}, which is
 *       replicated, so the quorum mints the epoch.
 *   <li>The coordinator advances the epoch on every {@code InitProducerId} call and
 *       reuses no value, so the epoch grows.
 *   <li>The broker rejects a write that carries a superseded epoch. It answers
 *       {@code INVALID_PRODUCER_EPOCH} or {@code PRODUCER_FENCED}. The cluster fences a
 *       deposed leader, and the leader does not fence itself.
 *   <li>{@code DescribeTransactions} reports the current producer id and producer epoch
 *       of a transactional id. Any process calls it, so a third party checks the
 *       authority of a writer with one request and no membership. See
 *       {@link io.krabka.streams.coordination.LeadershipStatus}.
 * </ul>
 *
 * <p>The lease adds no safety of its own. It is a liveness and anti-flap device. It
 * tells a standby when to stop waiting for a quiet holder. A wrong lease makes a
 * failover early or late. A wrong lease never makes two writers authoritative. Read
 * that inversion against a hand-rolled design, where the lease deadline decides who may
 * write. This package never asks the lease that question.
 *
 * <h2>The fencing token is a pair</h2>
 *
 * <p>The producer epoch is a {@code short} and it wraps. Kafka handles the exhaustion
 * with a fresh producer id and an epoch of zero. An implementation that compares the
 * epoch alone accepts a stale writer after about 32000 leadership changes.
 * {@link io.krabka.streams.coordination.FencingToken} holds the pair and compares the
 * producer id first.
 *
 * <h2>Rank comes from the log</h2>
 *
 * <p>A candidate appends a registration record to {@code __coordination_state}. The
 * offset of that record is the join sequence of the candidate, and log compaction keeps
 * the offset of every record it retains.
 * {@link io.krabka.streams.coordination.RoleState} folds that walk, and the roster it
 * builds is in offset order. A recovered node registers again, takes a higher offset,
 * and lands at the tail. That is the no-failback rule, and it needs no counter and no
 * coordinator. A rank from a configuration file would put the recovered node at the
 * front, and the node would then preempt the member that replaced it.
 *
 * <h2>The layers</h2>
 *
 * <ul>
 *   <li>{@link io.krabka.streams.coordination.CoordinationCodec} is the record codec of
 *       the frozen {@code __coordination_state} layouts.
 *   <li>{@link io.krabka.streams.coordination.Succession} holds the succession rules.
 *       They read a state, an instant, and a policy, and they perform no input and no
 *       output.
 *   <li>{@link io.krabka.streams.coordination.LeaseConfig} and
 *       {@link io.krabka.streams.coordination.LeaseTiming} are the lease clock, and
 *       {@link io.krabka.streams.coordination.Clock} is the seam a test replaces.
 *   <li>{@link io.krabka.streams.coordination.KafkaCoordinationTransport} carries the
 *       decisions to a cluster, and
 *       {@link io.krabka.streams.coordination.CoordinationClient} drives the loop.
 * </ul>
 *
 * <h2>Clock assumptions</h2>
 *
 * <p>The lease deadline is a wall-clock instant. The holder writes it, and a challenger
 * compares it against its own clock. Skew between the two clocks moves the failover
 * time by the size of the skew. Skew does not affect safety, for the reason the first
 * section gives. An operator that needs a bound on the failover time needs a bound on
 * the skew.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * Role role = Role.of("controller");
 * MemberId me = MemberId.of("node-1");
 * try (CoordinationClient client = new CoordinationClient(transport);
 *         Leadership leadership = client.acquire(role, me, Duration.ofMinutes(1))) {
 *     while (running) {
 *         if (leadership.renewDue()) {
 *             leadership.renew();
 *         }
 *         dispatch(leadership.token());
 *     }
 * } catch (FencedException lost) {
 *     controller.stop();
 * }
 * }</pre>
 */
package io.krabka.streams.coordination;
