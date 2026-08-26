package io.krabka.streams.coordination;

import java.util.Optional;

/** Builders that the succession and client tests share. */
final class CoordinationRecords {
    static final Role CONTROLLER = Role.of("controller");
    static final Role COMPACTOR = Role.of("compactor");

    private CoordinationRecords() {
    }

    static MemberId member(String name) {
        return MemberId.of(name);
    }

    static FencingToken token(short epoch) {
        return FencingToken.of(11L, epoch);
    }

    static CoordinationEntry registration(long offset, Role role, String name, long registeredAt) {
        return new CoordinationEntry(
                offset,
                CoordinationKey.registration(role, member(name)),
                Optional.of(new Registration(member(name), registeredAt)));
    }

    static CoordinationEntry deregistration(long offset, Role role, String name) {
        return new CoordinationEntry(
                offset, CoordinationKey.registration(role, member(name)), Optional.empty());
    }

    static CoordinationEntry lease(long offset, Role role, String name, long grantedAt, long deadline) {
        return new CoordinationEntry(
                offset,
                CoordinationKey.lease(role),
                Optional.of(new Lease(member(name), token((short) 1), grantedAt, deadline)));
    }

    static CoordinationEntry leaseTombstone(long offset, Role role) {
        return new CoordinationEntry(offset, CoordinationKey.lease(role), Optional.empty());
    }
}
