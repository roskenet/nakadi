package org.zalando.nakadi.validation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BCP47LanguageTagValidatorTest {
    private final BCP47LanguageTagValidator unit = new BCP47LanguageTagValidator();

    @ParameterizedTest
    @ValueSource(strings = {
            "en", "english", "german", "deutsch",
            "es-419", "rm-sursilv", "sr-Cyrl", "nan-Hant-TW", "yue-Hant-HK", "gsw-u-sd-chzh",
    })
    void testCorrectLanguageTag(final String input) {
        final var result = unit.validate(input);
        Assertions.assertTrue(
                result.isEmpty(),
                () -> String.format("input [%s] is expected to be valid", input));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "en-", "antarctic",
    })
    void testIncorrectInput(final String input) {
        final var result = unit.validate(input);
        Assertions.assertFalse(
                result.isEmpty(),
                () -> String.format("input [%s] is expected to be invalid", input));
    }
}
