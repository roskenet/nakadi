package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;

import java.util.Optional;
import java.util.UUID;

public class UUIDValidator implements FormatValidator {

    private final String formatName;

    public UUIDValidator(final String formatName) {
        this.formatName = formatName;
    }

    @Override
    public Optional<String> validate(final String input) {
        try {
            UUID.fromString(input);
            return Optional.empty();
        } catch (final IllegalArgumentException e) {
            return Optional.of(String.format("[%s] is not a valid uuid", input));
        }
    }

    @Override
    public String formatName() {
        return formatName;
    }
}
