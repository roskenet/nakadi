package org.zalando.nakadi.validation;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class UUIDValidatorTest {

    private final UUIDValidator uuidValidator = new UUIDValidator("test-event-type", "uuid");

    @Test
    public void testValidUUID() {
        assertEquals(
                Optional.empty(),
                uuidValidator.validate("123e4567-e89b-12d3-a456-426614174000"));
    }

    @Test
    public void testInvalidUUID() {
        assertEquals(
                Optional.empty(),
                uuidValidator.validate("invalid-uuid"));
    }
}
