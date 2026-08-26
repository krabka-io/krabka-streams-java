package io.krabka.streams.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HexFormat;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The {@code __coordination_state} wire format is frozen across {@code krabka-client-rs},
 * {@code krabka-streams-go}, and this module. These bytes are encoded straight from the layout the
 * coordination design freezes, independently of all three implementations, so a codec that drifts
 * fails here. The same vectors are asserted in the other two.
 *
 * <p>The fixture is role {@code controller}, member {@code node-1}, registered and granted at
 * {@code 1700000000000}, a deadline of {@code 1700000030000}, and the token {@code 4242:7}.
 */
final class CoordinationCodecGoldenTest {
    private static final Role ROLE = Role.of("controller");
    private static final MemberId MEMBER = MemberId.of("node-1");
    private static final long REGISTERED_AT = 1_700_000_000_000L;
    private static final long GRANTED_AT = 1_700_000_000_000L;
    private static final long DEADLINE = 1_700_000_030_000L;
    private static final FencingToken TOKEN = FencingToken.of(4242L, (short) 7);

    /**
     * Version 0, kind 0 (registration), role {@code controller}, member {@code node-1}. 24 bytes:
     * {@code 00 00 | 00 00 | 00 0A "controller" | 00 06 "node-1"}.
     */
    private static final String REGISTRATION_KEY = "00000000000a636f6e74726f6c6c657200066e6f64652d31";

    /**
     * Version 0, kind 1 (lease), role {@code controller}, and the empty member string. 18 bytes:
     * {@code 00 00 | 00 01 | 00 0A "controller" | 00 00}.
     */
    private static final String LEASE_KEY = "00000001000a636f6e74726f6c6c65720000";

    /**
     * Version 0, member {@code node-1}, registered at 1700000000000. 18 bytes:
     * {@code 00 00 | 00 06 "node-1" | 00 00 01 8B CF E5 68 00}.
     */
    private static final String REGISTRATION_VALUE = "000000066e6f64652d310000018bcfe56800";

    /**
     * Version 0, member {@code node-1}, producer id 4242, producer epoch 7, granted at
     * 1700000000000, deadline 1700000030000. 36 bytes.
     */
    private static final String LEASE_VALUE = "000000066e6f64652d31000000000000109200070000018bcfe568000000018bcfe5dd30";

    @Test
    void encodesTheFrozenRegistrationKey() {
        assertThat(hex(CoordinationCodec.encodeKey(CoordinationKey.registration(ROLE, MEMBER))))
                .isEqualTo(REGISTRATION_KEY);
    }

    @Test
    void encodesTheFrozenLeaseKey() {
        assertThat(hex(CoordinationCodec.encodeKey(CoordinationKey.lease(ROLE)))).isEqualTo(LEASE_KEY);
    }

    @Test
    void encodesTheFrozenRegistrationValue() {
        assertThat(hex(CoordinationCodec.encodeValue(new Registration(MEMBER, REGISTERED_AT))))
                .isEqualTo(REGISTRATION_VALUE);
    }

    @Test
    void encodesTheFrozenLeaseValue() {
        assertThat(hex(CoordinationCodec.encodeValue(new Lease(MEMBER, TOKEN, GRANTED_AT, DEADLINE))))
                .isEqualTo(LEASE_VALUE);
    }

    @Test
    void decodesTheFrozenRegistrationKey() {
        assertThat(CoordinationCodec.decodeKey(bytes(REGISTRATION_KEY)))
                .usingRecursiveComparison()
                .isEqualTo(CoordinationKey.registration(ROLE, MEMBER));
    }

    @Test
    void decodesTheFrozenLeaseKey() {
        assertThat(CoordinationCodec.decodeKey(bytes(LEASE_KEY)))
                .usingRecursiveComparison()
                .isEqualTo(CoordinationKey.lease(ROLE));
    }

    @Test
    void decodesTheFrozenRegistrationValue() {
        assertThat(CoordinationCodec.decodeValue(RecordKind.REGISTRATION, bytes(REGISTRATION_VALUE)))
                .usingRecursiveComparison()
                .isEqualTo(Optional.of(new Registration(MEMBER, REGISTERED_AT)));
    }

    @Test
    void decodesTheFrozenLeaseValue() {
        assertThat(CoordinationCodec.decodeValue(RecordKind.LEASE, bytes(LEASE_VALUE)))
                .usingRecursiveComparison()
                .isEqualTo(Optional.of(new Lease(MEMBER, TOKEN, GRANTED_AT, DEADLINE)));
    }

    @Test
    void writesAStringAsAPlainUtf8LengthAndNotAsJavaModifiedUtf8() {
        // DataOutputStream.writeUTF would prefix an unsigned 16-bit modified-UTF-8 length and would
        // encode a NUL as two bytes. The frozen layout writes an i16 byte length and plain UTF-8.
        byte[] key = CoordinationCodec.encodeKey(
                CoordinationKey.registration(Role.of("röle"), MemberId.of("m")));

        // "röle" is five UTF-8 bytes, and 0xC3 0xB6 is the two-byte form of U+00F6.
        assertThat(hex(key)).isEqualTo("00000000" + "0005" + "72c3b66c65" + "0001" + "6d");
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    private static byte[] bytes(String hex) {
        return HexFormat.of().parseHex(hex);
    }
}
