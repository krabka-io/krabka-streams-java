package io.krabka.streams.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CoordinationCodecTest {
    private static final Role ROLE = Role.of("controller");
    private static final MemberId MEMBER = MemberId.of("node-1");

    @Test
    void roundTripsARegistrationKey() {
        CoordinationKey key = CoordinationKey.registration(ROLE, MEMBER);

        assertThat(CoordinationCodec.decodeKey(CoordinationCodec.encodeKey(key)))
                .usingRecursiveComparison()
                .isEqualTo(key);
    }

    @Test
    void roundTripsALeaseKey() {
        CoordinationKey key = CoordinationKey.lease(ROLE);

        assertThat(CoordinationCodec.decodeKey(CoordinationCodec.encodeKey(key)))
                .usingRecursiveComparison()
                .isEqualTo(key);
    }

    @Test
    void roundTripsARegistrationValue() {
        Registration registration = new Registration(MEMBER, -42L);

        assertThat(CoordinationCodec.decodeValue(
                        RecordKind.REGISTRATION, CoordinationCodec.encodeValue(registration)))
                .usingRecursiveComparison()
                .isEqualTo(Optional.of(registration));
    }

    @Test
    void roundTripsALeaseValueAtTheBoundsOfItsFields() {
        Lease lease = new Lease(
                MemberId.of("m"),
                FencingToken.of(Long.MAX_VALUE, Short.MAX_VALUE),
                Long.MIN_VALUE,
                Long.MAX_VALUE);

        assertThat(CoordinationCodec.decodeValue(RecordKind.LEASE, CoordinationCodec.encodeValue(lease)))
                .usingRecursiveComparison()
                .isEqualTo(Optional.of(lease));
    }

    @Test
    void roundTripsANameOfTheMaximumLength() {
        CoordinationKey key = CoordinationKey.registration(
                Role.of("r".repeat(Role.MAX_LENGTH)), MemberId.of("m".repeat(MemberId.MAX_LENGTH)));

        assertThat(CoordinationCodec.decodeKey(CoordinationCodec.encodeKey(key)))
                .usingRecursiveComparison()
                .isEqualTo(key);
    }

    @Test
    void roundTripsANonAsciiNameThroughPlainUtf8() {
        CoordinationKey key = CoordinationKey.registration(Role.of("rôle"), MemberId.of("nœud-1"));

        assertThat(CoordinationCodec.decodeKey(CoordinationCodec.encodeKey(key)))
                .usingRecursiveComparison()
                .isEqualTo(key);
    }

    @Test
    void readsANullValueAsATombstoneForEitherKind() {
        assertThat(CoordinationCodec.decodeValue(RecordKind.REGISTRATION, null)).isEmpty();
        assertThat(CoordinationCodec.decodeValue(RecordKind.LEASE, null)).isEmpty();
    }

    @Test
    void readsANullOrEmptyValueAsATombstone() {
        assertThat(CoordinationCodec.isTombstone(null)).isTrue();
        assertThat(CoordinationCodec.isTombstone(new byte[0])).isTrue();
        assertThat(CoordinationCodec.isTombstone(new byte[] {0, 0})).isFalse();
    }

    static Stream<Arguments> malformedKeys() {
        return Stream.of(
                Arguments.of("an empty buffer", ""),
                Arguments.of("a buffer that stops after the version", "0000"),
                Arguments.of("a key with no member string", "00000000000a636f6e74726f6c6c6572"),
                Arguments.of("a key with trailing bytes", "00000000000a636f6e74726f6c6c657200066e6f64652d3100"),
                Arguments.of("an unknown version", "00010000000a636f6e74726f6c6c657200066e6f64652d31"),
                Arguments.of("an unknown record kind", "00000009000a636f6e74726f6c6c657200066e6f64652d31"),
                Arguments.of("a negative string length", "00000000ffff636f6e74726f6c6c657200066e6f64652d31"),
                Arguments.of("a string length past the buffer", "0000000000ff636f6e74726f6c6c6572"),
                Arguments.of("a string that is not UTF-8", "0000000000026180"),
                Arguments.of("a lease key that names a member", "00000001000a636f6e74726f6c6c657200066e6f64652d31"),
                Arguments.of("an empty role", "0000000000000000"),
                Arguments.of("an empty member on a registration key", "00000000000a636f6e74726f6c6c65720000"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedKeys")
    void rejectsAMalformedKey(String fault, String hex) {
        byte[] bytes = HexFormat.of().parseHex(hex);

        assertThatThrownBy(() -> CoordinationCodec.decodeKey(bytes))
                .describedAs(fault)
                .isInstanceOf(CoordinationException.class);
    }

    static Stream<Arguments> malformedRegistrationValues() {
        return Stream.of(
                Arguments.of("a value with no member", "0000"),
                Arguments.of("a value with no timestamp", "000000066e6f64652d31"),
                Arguments.of("a value with trailing bytes", "000000066e6f64652d310000018bcfe5680000"),
                Arguments.of("an unknown version", "000100066e6f64652d310000018bcfe56800"),
                Arguments.of("an empty member", "000000000000000000000000"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedRegistrationValues")
    void rejectsAMalformedRegistrationValue(String fault, String hex) {
        byte[] bytes = HexFormat.of().parseHex(hex);

        assertThatThrownBy(() -> CoordinationCodec.decodeValue(RecordKind.REGISTRATION, bytes))
                .describedAs(fault)
                .isInstanceOf(CoordinationException.class);
    }

    static Stream<Arguments> malformedLeaseValues() {
        return Stream.of(
                Arguments.of("a value with no token", "000000066e6f64652d31"),
                Arguments.of(
                        "a value with no deadline",
                        "000000066e6f64652d31000000000000109200070000018bcfe56800"),
                Arguments.of(
                        "a value with trailing bytes",
                        "000000066e6f64652d31000000000000109200070000018bcfe568000000018bcfe5dd3000"),
                Arguments.of(
                        "a negative producer id",
                        "000000066e6f64652d31ffffffffffffffff00070000018bcfe568000000018bcfe5dd30"),
                Arguments.of(
                        "a negative producer epoch",
                        "000000066e6f64652d310000000000001092ffff0000018bcfe568000000018bcfe5dd30"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedLeaseValues")
    void rejectsAMalformedLeaseValue(String fault, String hex) {
        byte[] bytes = HexFormat.of().parseHex(hex);

        assertThatThrownBy(() -> CoordinationCodec.decodeValue(RecordKind.LEASE, bytes))
                .describedAs(fault)
                .isInstanceOf(CoordinationException.class);
    }

    @Test
    void namesTheRecordPartThatFailedToDecode() {
        assertThatThrownBy(() -> CoordinationCodec.decodeKey(HexFormat.of().parseHex("0001")))
                .hasMessage("malformed coordination state key: unsupported version 1");
        assertThatThrownBy(() ->
                        CoordinationCodec.decodeValue(RecordKind.LEASE, HexFormat.of().parseHex("0001")))
                .hasMessage("malformed coordination state lease value: unsupported version 1");
        assertThatThrownBy(() -> CoordinationCodec.decodeValue(
                        RecordKind.REGISTRATION, HexFormat.of().parseHex("0001")))
                .hasMessage("malformed coordination state registration value: unsupported version 1");
    }

    @Test
    void reportsAnErrorForEveryArbitraryInputAndRaisesNoOtherFailure() {
        Random random = new Random(20_260_826L);
        for (int attempt = 0; attempt < 5_000; attempt++) {
            byte[] bytes = new byte[random.nextInt(48)];
            random.nextBytes(bytes);
            assertThatDecodesOrReportsAnError(bytes);
        }
    }

    private static void assertThatDecodesOrReportsAnError(byte[] bytes) {
        assertThat(outcomeOf(() -> CoordinationCodec.decodeKey(bytes))).isNull();
        for (RecordKind kind : RecordKind.values()) {
            assertThat(outcomeOf(() -> CoordinationCodec.decodeValue(kind, bytes))).isNull();
        }
    }

    /** Runs a decode and returns the failure that is not a {@link CoordinationException}. */
    private static Throwable outcomeOf(Runnable decode) {
        try {
            decode.run();
            return null;
        } catch (CoordinationException reported) {
            return null;
        } catch (RuntimeException unexpected) {
            return unexpected;
        }
    }

    @Test
    void doesNotAliasTheCallerBuffer() {
        byte[] key = CoordinationCodec.encodeKey(CoordinationKey.registration(ROLE, MEMBER));
        CoordinationKey decoded = CoordinationCodec.decodeKey(key);
        Arrays.fill(key, (byte) 0);

        assertThat(decoded)
                .usingRecursiveComparison()
                .isEqualTo(CoordinationKey.registration(ROLE, MEMBER));
    }
}
