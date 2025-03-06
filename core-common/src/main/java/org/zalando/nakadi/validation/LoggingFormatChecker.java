package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class LoggingFormatChecker implements FormatValidator {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingFormatChecker.class);

    private final FormatValidator validator;
    private final String eventTypeName;
    private boolean loggedOnce = false;

    public LoggingFormatChecker(final FormatValidator validator, final String eventTypeName) {
        this.validator = validator;
        this.eventTypeName = eventTypeName;
    }

    @Override
    public Optional<String> validate(final String input) {
        final Optional<String> result = validator.validate(input);
        if (result.isPresent() && !loggedOnce) {
            loggedOnce = true;
            LOG.warn("[event-type={}][format={}]: {}", eventTypeName, validator.formatName(), result.get());
        }
        return Optional.empty();
    }

    @Override
    public String formatName() {
        return validator.formatName();
    }
}
