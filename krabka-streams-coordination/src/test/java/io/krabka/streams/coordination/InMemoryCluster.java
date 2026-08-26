package io.krabka.streams.coordination;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link CoordinationTransport} that models one partition of the coordination topic and one
 * transaction coordinator, with no broker.
 *
 * <p>The coordinator mints a strictly increasing epoch for every role, and a lease write under a
 * superseded token fails with {@link FencedException}. That is the only fence in the design, so a
 * test that drives this transport exercises the same loss path a cluster produces.
 */
final class InMemoryCluster implements CoordinationTransport {
    private final List<CoordinationEntry> log = new ArrayList<>();
    private final Map<Role, FencingToken> minted = new HashMap<>();
    private final Map<Role, Short> epochs = new HashMap<>();
    private boolean closed;

    @Override
    public FencingToken acquireEpoch(Role role) {
        short next = (short) (epochs.merge(role, (short) 1, (held, one) -> (short) (held + one)));
        FencingToken token = FencingToken.of(100L, next);
        minted.put(role, token);
        return token;
    }

    @Override
    public List<CoordinationEntry> readRoleRecords(Role role) {
        return log.stream().filter(entry -> entry.key().role().equals(role)).toList();
    }

    @Override
    public void register(Role role, MemberId member, long registeredAtMillis) {
        append(
                CoordinationKey.registration(role, member),
                Optional.of(new Registration(member, registeredAtMillis)));
    }

    @Override
    public void writeLease(Role role, FencingToken token, Lease lease) {
        requireCurrent(role, token);
        append(CoordinationKey.lease(role), Optional.of(lease));
    }

    @Override
    public void clearLease(Role role, FencingToken token) {
        requireCurrent(role, token);
        append(CoordinationKey.lease(role), Optional.empty());
    }

    @Override
    public Optional<FencingToken> describe(Role role) {
        return Optional.ofNullable(minted.get(role));
    }

    @Override
    public void close() {
        closed = true;
    }

    boolean closed() {
        return closed;
    }

    /** Appends a record the way another process would, so a test seeds a partition. */
    void append(CoordinationKey key, Optional<CoordinationValue> value) {
        log.add(new CoordinationEntry(log.size(), key, value));
    }

    private void requireCurrent(Role role, FencingToken token) {
        if (!token.equals(minted.get(role))) {
            throw new FencedException(role, null);
        }
    }
}
