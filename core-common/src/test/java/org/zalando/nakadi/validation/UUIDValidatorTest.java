package org.zalando.nakadi.validation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;
import java.util.UUID;

public class UUIDValidatorTest {

    private final UUIDValidator uuidValidator = new UUIDValidator("uuid");

    @Test
    public void testValidUUID() {
        Assertions.assertTrue(
                uuidValidator.validate("123e4567-e89b-12d3-a456-426614174000").isEmpty());
        for (int i = 0; i < 10; ++i) {
            final String uuid = UUID.randomUUID().toString();
            Assertions.assertTrue(uuidValidator.validate(uuid).isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "123",
            "123e4567-xyz1-klmn-a456-426614174000",
            "123e4567-e89b-12d3-a456--26614174000",
            "a-b-c-d-e",
            "a-0b-00c-000d-000000000000000000000e",
    })
    public void testInvalidUUID(final String value) {
        Assertions.assertEquals(
                Optional.of(String.format("[%s] is not a valid uuid", value)),
                uuidValidator.validate(value));
    }
}
