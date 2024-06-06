package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Optional;
import java.util.UUID;

public class UUIDValidator implements FormatValidator {
    public static Logger logger = LoggerFactory.getLogger(UUIDValidator.class);

    @Override
    public Optional<String> validate(final String input) {
        if (input == null) {
            logWarn("The input is null");
            return Optional.empty();
        }
        try {
            UUID.fromString(input);
        } catch (final IllegalArgumentException e) {
            logWarn(input + "is an invalid UUID");
            return Optional.empty();
        }
        return Optional.empty();
    }

    @Override
    public String formatName() {
        return "uuid";
    }

    protected void logWarn(final String message) {
        logger.warn(message);
    }
}
