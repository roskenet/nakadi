package org.zalando.nakadi.validation;

import org.everit.json.schema.internal.EmailFormatValidator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EmailValidatorTest {

    private final EmailFormatValidator emailValidator = new EmailFormatValidator();

    @ParameterizedTest
    @CsvSource({
            // --- VALID EMAILS ---
            "test@example.com, true",
            "firstname.lastname@zalando.de, true",
            "test.email+alias@gmail.com, true",
            "email@sub.domain.co.uk, true",

            // --- INVALID EMAILS ---
            "plainaddress, false",          // Missing @ and domain
            "@missing-local-part.com, false", // Missing local part
            "test.example.com@, false",       // Missing domain part
            "test@.example.com, false",      // Domain starts with a dot
            "test@example..com, false",     // Domain has consecutive dots
            "test @example.com, false",     // Whitespace is not allowed
            "test@example, false",           // Missing Top-Level Domain (TLD)
            "e.bee@ext.bettyblu, false" // Invalid TLD
    })
    public void testEmailValidation(final String email, final boolean shouldBeValid) {
        final var result = emailValidator.validate(email);

        assertEquals(shouldBeValid, result.isEmpty(),
                "Validation result for '" + email + "' did not match expected: " + shouldBeValid);
    }
}
