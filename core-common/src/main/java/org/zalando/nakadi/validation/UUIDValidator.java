package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;

public class UUIDValidator implements FormatValidator {

    private static final Logger LOG = LoggerFactory.getLogger(UUIDValidator.class);

    private final String eventTypeName;

    // TODO: remove when actually enabling the validation
    public UUIDValidator(final String eventTypeName) {
        this.eventTypeName = eventTypeName;
    }

    @Override
    public Optional<String> validate(final String input) {
        try {
            UUID.fromString(input);
            return Optional.empty();
        } catch (final IllegalArgumentException e) {
            LOG.warn("validating event schema for '{}' - not a valid UUID: {}", eventTypeName, input);
            return Optional.empty();
        }
    }

    @Override
    public String formatName() {
        return "uuid";
    }
}
