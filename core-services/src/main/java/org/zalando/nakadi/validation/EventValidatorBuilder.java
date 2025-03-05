package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;

import java.util.Optional;

@Component
public class EventValidatorBuilder {

    // NOTE: org.everit.json also adds additional format validators that are not listed here explicitly!
    private static final FormatValidator[] ASSERTED_FORMAT_VALIDATORS = {
            new RFC3339DateTimeValidator(),
            new ISO4217CurrencyCodeValidator(),
            new ISO4217CurrencyCodeValidator("ISO-4217"),
    };
    private static final FormatValidator[] PROSPECTIVE_FORMAT_VALIDATORS = {
            new UUIDValidator("uuid"),
            new UUIDValidator("UUID"),
    };
    private static final JsonSchemaValidator METADATA_VALIDATOR = new MetadataValidator();
    private final JsonSchemaEnrichment loader;

    @Autowired
    public EventValidatorBuilder(final JsonSchemaEnrichment loader) {
        this.loader = loader;
    }

    public JsonSchemaValidator build(final EventType eventType) {
        final Optional<EventTypeSchema> jsonSchema = eventType.getLatestSchemaByType(EventTypeSchema.Type.JSON_SCHEMA);
        if (jsonSchema.isEmpty()) {
            throw new NoSuchSchemaException("No json_schema found for event type: " + eventType.getName());
        }

        final SchemaLoader.SchemaLoaderBuilder builder = SchemaLoader.builder()
                .schemaJson(loader.effectiveSchema(eventType, jsonSchema.get().getSchema()));
        final Schema schema = addFormatValidators(builder, eventType.getName())
                .build()
                .load()
                .build();

        final JsonSchemaValidator baseValidator = new SchemaValidator(schema);

        return eventType.getCategory() == EventCategory.DATA || eventType.getCategory() == EventCategory.BUSINESS
                ? new ChainingValidator(baseValidator, METADATA_VALIDATOR)
                : baseValidator;
    }

    private SchemaLoader.SchemaLoaderBuilder addFormatValidators(
            final SchemaLoader.SchemaLoaderBuilder builder, final String eventTypeName) {
        for (final var validator: ASSERTED_FORMAT_VALIDATORS) {
            builder.addFormatValidator(validator);
        }
        for (final var validator: PROSPECTIVE_FORMAT_VALIDATORS) {
            builder.addFormatValidator(new LoggingFormatChecker(validator, eventTypeName));
        }
        return builder;
    }

    private static class ChainingValidator implements JsonSchemaValidator {
        private final JsonSchemaValidator first;
        private final JsonSchemaValidator next;

        private ChainingValidator(final JsonSchemaValidator first, final JsonSchemaValidator next) {
            this.first = first;
            this.next = next;
        }

        @Override
        public Optional<ValidationError> validate(final JSONObject event) {
            return first.validate(event)
                    .or(() -> next.validate(event));
        }

    }

    private static class MetadataValidator implements JsonSchemaValidator {
        private final RFC3339DateTimeValidator dateTimeValidator = new RFC3339DateTimeValidator();

        @Override
        public Optional<ValidationError> validate(final JSONObject event) {
            return Optional
                    .ofNullable(event.optJSONObject("metadata"))
                    .map(metadata -> metadata.optString("occurred_at"))
                    .flatMap(dateTimeValidator::validate)
                    .map(e -> new ValidationError("#/metadata/occurred_at:" + e));
        }

    }

    private static class SchemaValidator implements JsonSchemaValidator {
        final Schema schema;

        private SchemaValidator(final Schema schema) {
            this.schema = schema;
        }

        @Override
        public Optional<ValidationError> validate(final JSONObject evt) {
            try {
                schema.validate(evt);
                return Optional.empty();
            } catch (final ValidationException e) {
                final StringBuilder builder = new StringBuilder();
                recursiveCollectErrors(e, builder);
                return Optional.of(new ValidationError(builder.toString()));
            }
        }

        private static void recursiveCollectErrors(final ValidationException e, final StringBuilder builder) {
            builder.append(e.getMessage());

            e.getCausingExceptions().forEach(causingException -> {
                builder.append("\n");
                recursiveCollectErrors(causingException, builder);
            });
        }
    }
}
