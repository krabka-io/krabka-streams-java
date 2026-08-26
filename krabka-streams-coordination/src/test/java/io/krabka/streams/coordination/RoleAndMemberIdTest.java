package io.krabka.streams.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RoleAndMemberIdTest {
    @Test
    void acceptsANameOfTheMaximumLength() {
        assertThat(Role.of("r".repeat(Role.MAX_LENGTH)).name()).hasSize(Role.MAX_LENGTH);
        assertThat(MemberId.of("m".repeat(MemberId.MAX_LENGTH)).id()).hasSize(MemberId.MAX_LENGTH);
    }

    @Test
    void rejectsAnEmptyName() {
        assertThatThrownBy(() -> Role.of(""))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("a coordination role must not be empty");
        assertThatThrownBy(() -> MemberId.of(""))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("a coordination member id must not be empty");
    }

    @Test
    void rejectsANamePastItsByteBound() {
        assertThatThrownBy(() -> Role.of("r".repeat(Role.MAX_LENGTH + 1)))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("a coordination role of 250 bytes is longer than the 249-byte maximum");
    }

    @Test
    void countsBytesAndNotCharactersAgainstTheBound() {
        // Each "é" is two UTF-8 bytes, so 125 of them pass the bound and 126 do not.
        assertThat(Role.of("é".repeat(124)).bytes()).hasSize(248);
        assertThatThrownBy(() -> Role.of("é".repeat(125)))
                .isInstanceOf(CoordinationException.class)
                .hasMessage("a coordination role of 250 bytes is longer than the 249-byte maximum");
    }

    @Test
    void returnsTheUtf8BytesOfTheName() {
        assertThat(Role.of("rôle").bytes()).isEqualTo("rôle".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void treatsTwoNamesOfOneTextAsEqual() {
        assertThat(Role.of("controller"))
                .isEqualTo(Role.of("controller"))
                .hasSameHashCodeAs(Role.of("controller"))
                .isNotEqualTo(Role.of("dispatcher"))
                .isNotEqualTo("controller")
                .hasToString("controller");
        assertThat(MemberId.of("node-1"))
                .isEqualTo(MemberId.of("node-1"))
                .hasSameHashCodeAs(MemberId.of("node-1"))
                .isNotEqualTo(MemberId.of("node-2"))
                .hasToString("node-1");
    }

    @Test
    void ordersNamesByTheirText() {
        assertThat(Role.of("a")).isLessThan(Role.of("b"));
        assertThat(MemberId.of("node-2")).isGreaterThan(MemberId.of("node-1"));
    }

    @Test
    void rejectsANullName() {
        assertThatThrownBy(() -> Role.of(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> MemberId.of(null)).isInstanceOf(NullPointerException.class);
    }
}
