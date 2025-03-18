package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;

import java.util.IllformedLocaleException;
import java.util.Locale;
import java.util.Optional;

public class BCP47LanguageTagValidator implements FormatValidator {
    @Override
    public Optional<String> validate(final String input) {
        try {
            new Locale.Builder().setLanguageTag(input);
        } catch (IllformedLocaleException e) {
            return Optional.of(String.format("[%s] is not a valid IETF BCP 47 language tag", input));
        }
        return Optional.empty();
    }

    @Override
    public String formatName() {
        return "bcp47";
    }
}
