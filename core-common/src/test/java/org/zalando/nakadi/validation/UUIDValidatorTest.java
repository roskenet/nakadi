package org.zalando.nakadi.validation;

import org.junit.Test;
import org.slf4j.Logger;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class UUIDValidatorTest {

    private final UUIDValidator uuidValidator = new UUIDValidator();

    @Test
    public void testNullInput() {
        final var result = uuidValidator.validate(null);
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testValidUUID() {
        final var validUUID = "123e4567-e89b-12d3-a456-426614174000";
        final var result = uuidValidator.validate(validUUID);
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testInvalidUUID() {
        final var invalidUUID = "invalid-uuid";
        final var result = uuidValidator.validate(invalidUUID);
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testLoggingOnNullInput() {
        final var mockLogger = mock(Logger.class);
        final var validatorWithMockLogger = new UUIDValidator();
        validatorWithMockLogger.logger = mockLogger;

        validatorWithMockLogger.validate(null);
        verify(mockLogger).warn("The input is null");
    }

    @Test
    public void testLoggingOnInvalidUUID() {
        final var mockLogger = mock(Logger.class);
        final var validatorWithMockLogger = new UUIDValidator();
        validatorWithMockLogger.logger = mockLogger;

        validatorWithMockLogger.validate("invalid-uuid");
        verify(mockLogger).warn("invalid-uuidis an invalid UUID");
    }
}
