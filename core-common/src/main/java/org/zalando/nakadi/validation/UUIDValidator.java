package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;

import java.util.Optional;
import java.util.regex.Pattern;

public class UUIDValidator implements FormatValidator {
    private static final Pattern UUID_REGEX =
            Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    private final String formatName;

    public UUIDValidator(final String formatName) {
        this.formatName = formatName;
    }

    @Override
    public Optional<String> validate(final String input) {
        if (UUID_REGEX.matcher(input).matches()) {
            return Optional.empty();
        }
        return Optional.of(String.format("[%s] is not a valid uuid", input));
    }

    @Override
    public String formatName() {
        return formatName;
    }

    public static boolean isStrictlyValid(final String input) {
        return UUID_REGEX.matcher(input).matches();
    }
}
