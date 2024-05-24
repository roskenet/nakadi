package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;

import java.util.Optional;
import java.util.UUID;


public class UUIDValidator implements FormatValidator {

    @Override
    public Optional<String> validate(final String input) {
        try {
            UUID.fromString(input);
        } catch (final IllegalArgumentException e) {
            return Optional.of(String.format("%s is an invalid UUID", input));
        } catch (final NullPointerException e) {
            return Optional.of("The input string is null");
        }
        return Optional.empty();
    }

    @Override
    public String formatName() {
        return "uuid";
    }
}
