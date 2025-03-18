package org.zalando.nakadi.validation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Locale;
import java.util.stream.Stream;

class ISO3166Alpha2CountryCodeValidatorTest {
    private final ISO3166Alpha2CountryCodeValidator unit = new ISO3166Alpha2CountryCodeValidator();

    @Test
    void testCorrectCountryCodes() {
        for (final var countryCode: Locale.getISOCountries(Locale.IsoCountryCode.PART1_ALPHA2)) {
            final var result = unit.validate(countryCode);
            Assertions.assertTrue(
                    result.isEmpty(),
                    () -> String.format("country code [%s] expected to be valid", countryCode));
        }
    }

    @ParameterizedTest
    @MethodSource("getInvalidInput")
    void testIncorrectInput(final String input) {
        final var result = unit.validate(input);
        Assertions.assertFalse(
                result.isEmpty(),
                () -> String.format("input [%s] is expected to be invalid", input));
    }

    private static Stream<String> getInvalidInput() {
        return Stream.of(
                "AA", "AC", "AH", "AN", "AP",
                "OO",
                "QM", "QZ",
                "UK", "UN",
                "XA", "XB", "XC", "XX", "XY", "XZ",
                "de", "De", "dE", "DEU", " DE", "DE "
        );
    }
}