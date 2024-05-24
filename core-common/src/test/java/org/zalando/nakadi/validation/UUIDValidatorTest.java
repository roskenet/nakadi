package org.zalando.nakadi.validation;

import org.junit.Test;
import org.zalando.nakadi.utils.IsOptional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class UUIDValidatorTest {
    private final UUIDValidator validator = new UUIDValidator();

    @Test
    public void testInvalidInput() {
        final var invalidInput = new String[]{
                "garbage",
                "g95d6d85-472f-4c93-9a2f-a1fc07799467", // contains invalid character 'g'
                "095d6d85-472f-4c93-9a2f-a1fc077994677", // one character extra
                "095D6D85472F4C939A2FA1FC07799467", // UUID without '-'s
                "",
                null
        };

        for (final String invalid : invalidInput) {
            assertThat(invalid, validator.validate(invalid), IsOptional.isPresent());
        }
    }

    @Test
    public void testValidInput() {
        final var validInput = new String[]{
                "095d6d85-472f-4c93-9a2f-a1fc07799467",
                "095D6D85-472F-4C93-9A2F-A1FC07799467", // uppercase
                "095D6D85-472f-4c93-9A2F-A1FC07799467", // upper and lower cases
                "550e8400-e29b-41d4-a716-446655440000", // another valid UUID
                "550E8400-e29B-41D4-A716-446655440000", // valid UUID in uppercase
        };

        for (final String valid : validInput) {
            assertThat(valid, validator.validate(valid), IsOptional.isAbsent());
        }
    }

    @Test
    public void testFormatName() {
        assertEquals(validator.formatName(), "uuid");
    }
}
