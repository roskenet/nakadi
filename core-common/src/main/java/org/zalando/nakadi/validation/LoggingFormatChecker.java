package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Predicate;

public class LoggingFormatChecker implements FormatValidator {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingFormatChecker.class);

    private final FormatValidator validator;
    private final String eventTypeName;
    private boolean loggedOnce = false;
    private final Predicate<String> isFormatAsserted;
    private boolean checkStrictUuidAndLog;

    public LoggingFormatChecker(
            final FormatValidator validator,
            final String eventTypeName,
            final Predicate<String> isFormatAsserted) {
        this.validator = validator;
        this.eventTypeName = eventTypeName;
        this.isFormatAsserted = isFormatAsserted;
        this.checkStrictUuidAndLog = "uuid".equals(validator.formatName()) || "UUID".equals(validator.formatName());
    }

    @Override
    public Optional<String> validate(final String input) {
        final Optional<String> result = validator.validate(input);
        if (result.isPresent()) {
            if (isFormatAsserted.test(formatName())) {
                loggedOnce = false;
                return result;
            }
            if (!loggedOnce) {
                loggedOnce = true;
                LOG.warn("[event-type={}][format={}]: {}", eventTypeName, formatName(), result.get());
            }
        }
        if (checkStrictUuidAndLog) {
            // TODO: make this proper check in UUIDValidator, after confirming there are no such logs.
            if (!UUIDValidator.isStrictlyValid(input)) {
                checkStrictUuidAndLog = false;
                LOG.warn("[event-type={}][format={}]: [{}] is not strict uuid", eventTypeName, formatName(), input);
            }
        }
        return Optional.empty();
    }

    @Override
    public String formatName() {
        return validator.formatName();
    }
}
