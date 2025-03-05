package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;

import java.util.Locale;
import java.util.Optional;

public class ISO3166Alpha2CountryCodeValidator implements FormatValidator {
    @Override
    public Optional<String> validate(final String input) {
        if (Locale.getISOCountries(Locale.IsoCountryCode.PART1_ALPHA2).contains(input)) {
            return Optional.empty();
        }
        return Optional.of(String.format("[%s] is not a valid ISO 3166-1 alpha-2 country code", input));
    }

    @Override
    public String formatName() {
        return "iso-3166-alpha-2";
    }
}
