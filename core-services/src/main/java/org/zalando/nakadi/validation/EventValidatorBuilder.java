package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.internal.DateFormatValidator;
import org.everit.json.schema.internal.RegexFormatValidator;
import org.everit.json.schema.internal.TimeFormatValidator;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.service.FeatureToggleService;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

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
            new DateFormatValidator(), // format: date
            new TimeFormatValidator(), // format: time
            new RegexFormatValidator(), // format: regex
            new ISO3166Alpha2CountryCodeValidator(), // format: iso-3166-alpha-2
            new BCP47LanguageTagValidator(), // format: bcp47
    };
    private static final Map<String, Feature> FORMAT_TO_FEATURE_MAPPING;
    static {
        FORMAT_TO_FEATURE_MAPPING = new HashMap<>();
        for (final var validator: PROSPECTIVE_FORMAT_VALIDATORS) {
            final var formatName = validator.formatName();
            final var feature = Feature.valueOf(
                    "ASSERT_JSON_FORMAT_" + formatName.toUpperCase().replace('-', '_'));
            FORMAT_TO_FEATURE_MAPPING.put(formatName, feature);
        }
    }

    private static final JsonSchemaValidator METADATA_VALIDATOR = new MetadataValidator();
    private final JsonSchemaEnrichment loader;
    private final FeatureToggleService featureToggleService;

    @Autowired
    public EventValidatorBuilder(final JsonSchemaEnrichment loader, final FeatureToggleService featureToggleService) {
        this.loader = loader;
        this.featureToggleService = featureToggleService;
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
        final Predicate<String> isFormatAsserted =
                (formatName) -> featureToggleService.isFeatureEnabled(FORMAT_TO_FEATURE_MAPPING.get(formatName));
        for (final var validator: PROSPECTIVE_FORMAT_VALIDATORS) {
            builder.addFormatValidator(new LoggingFormatChecker(validator, eventTypeName, isFormatAsserted));
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
