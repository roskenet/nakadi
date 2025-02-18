package org.zalando.nakadi.service.validation;

import com.google.common.annotations.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.OptInTeamsConfig;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;
import org.zalando.nakadi.service.FeatureToggleService;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.zalando.nakadi.service.auth.AuthorizationResourceMapping.DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION;

@Component
public class EventTypeAnnotationsValidator {
    private static final Pattern DATA_LAKE_ANNOTATIONS_PERIOD_PATTERN = Pattern.compile(
            "^(([1-9][0-9]*)((d|w|m|y)|(\\s(day|week|month|year)s?)))|(unlimited)$");

    static final String DATA_LAKE_RETENTION_PERIOD_ANNOTATION = "datalake.zalando.org/retention-period";
    static final String DATA_LAKE_RETENTION_REASON_ANNOTATION = "datalake.zalando.org/retention-period-reason";
    static final String DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION = "datalake.zalando.org/materialize-events";

    static final String DATA_LAKE_ANNOTATIONS_DOCUMENT_URL =
            "https://docs.google.com/document/d/1-SwwpwUqauc_pXu-743YA1gO8l5_R_Gf4nbYml1ySiI"
            + "/edit#heading=h.kmvigbxbn1dj";

    private final FeatureToggleService featureToggleService;
    private final AuthorizationService authorizationService;
    private final OptInTeamsConfig optInTeamsConfig;
    private final ApplicationService applicationService;
    private final List<String> enforcedAuthSubjects;

    @Autowired
    public EventTypeAnnotationsValidator(
            final FeatureToggleService featureToggleService,
            final AuthorizationService authorizationService,
            final OptInTeamsConfig optInTeamsConfig,
            final ApplicationService applicationService,
            @Value("${nakadi.data_lake.annotations.enforced_auth_subjects:}") final List<String> enforcedAuthSubjects
    ) {
        this.featureToggleService = featureToggleService;
        this.authorizationService = authorizationService;
        this.enforcedAuthSubjects = enforcedAuthSubjects;
        this.applicationService = applicationService;
        this.optInTeamsConfig = optInTeamsConfig;
    }

    public void validateAnnotations(
            final EventTypeBase oldEventType,
            @NotNull final EventTypeBase newEventType) throws InvalidEventTypeException {

        final var oldAnnotations = oldEventType == null ?
                null : Optional.ofNullable(oldEventType.getAnnotations()).orElseGet(Collections::emptyMap);
        final var newAnnotations = Optional.ofNullable(newEventType.getAnnotations())
                .orElseGet(Collections::emptyMap);
        validateDataComplianceAnnotations(oldAnnotations, newAnnotations, newEventType.getOwningApplication());
        validateDataLakeAnnotations(oldAnnotations, newAnnotations);
    }

    @VisibleForTesting
    public void validateDataComplianceAnnotations(
            // null iff we're validating a new event type (i.e. there is no old event type)
            final Map<String, String> oldAnnotations,
            @NotNull final Map<String, String> annotations,
            final String owningApplication) {

        final Set<String> allowedValues = Set.of("none", "aspd", "mcf-aspd");
        final var aspdClassification = annotations.get(DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION);

        if (annotations.containsKey(DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION)
                && aspdClassification == null) {
            throw new InvalidEventTypeException(
                    String.format("Annotation %s cannot have null value",
                            DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION)
            );
        }

        if (aspdClassification != null && !allowedValues.contains(aspdClassification)) {
            throw new InvalidEventTypeException(
                    "Annotation " + DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION
                            + " is not valid. Provided value: \""
                            + aspdClassification
                            + "\". The allowed values are: \"none\", \"aspd\" or \"mcf-aspd\".");
        }

        if (isDataComplianceAnnotationRequired(oldAnnotations, owningApplication)) {
            if (aspdClassification == null) {
                throw new InvalidEventTypeException(
                        "Annotation " + DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION + " is required");
            }
        }
    }

    private boolean isDataComplianceAnnotationRequired(
            final Map<String, String> oldAnnotations,
            final String owningApplication) {
        if (oldAnnotations == null) { // Triggered when a new event type is created
            try {
                return owningApplication != null &&
                        (applicationService
                                .getOwningTeamId(owningApplication)
                                .filter(teamId -> optInTeamsConfig.getOptInTeams().contains(teamId))
                                .isPresent());
            } catch (final PluginException e) {
                throw new InvalidEventTypeException(e);
            }
        } else {
            return oldAnnotations.containsKey(DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION);
        }
    }

    @VisibleForTesting
    void validateDataLakeAnnotations(
            // null iff we're validating a new event type (i.e. there is no old event type)
            final Map<String, String> oldAnnotations,
            @NotNull final Map<String, String> annotations) {
        final var materializeEvents = annotations.get(DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION);
        final var retentionPeriod = annotations.get(DATA_LAKE_RETENTION_PERIOD_ANNOTATION);

        if (materializeEvents != null) {
            if (!materializeEvents.equals("off") && !materializeEvents.equals("on")) {
                throw new InvalidEventTypeException(
                        "Annotation " + DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION
                                + " is not valid. Provided value: \""
                                + materializeEvents
                                + "\". Possible values are: \"on\" or \"off\".");
            }
            if (materializeEvents.equals("on")) {
                if (retentionPeriod == null) {
                    throw new InvalidEventTypeException("Annotation " + DATA_LAKE_RETENTION_PERIOD_ANNOTATION
                            + " is required, when " + DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION + " is \"on\".");
                }
            }
        }

        if (retentionPeriod != null) {
            final var retentionReason = annotations.get(DATA_LAKE_RETENTION_REASON_ANNOTATION);
            if (retentionReason == null || retentionReason.isEmpty()) {
                throw new InvalidEventTypeException(
                        "Annotation " + DATA_LAKE_RETENTION_REASON_ANNOTATION + " is required, when "
                                + DATA_LAKE_RETENTION_PERIOD_ANNOTATION + " is specified.");
            }

            final Matcher matcher = DATA_LAKE_ANNOTATIONS_PERIOD_PATTERN.matcher(retentionPeriod);
            if (!matcher.matches()) {
                throw new InvalidEventTypeException(
                        "Annotation " + DATA_LAKE_RETENTION_PERIOD_ANNOTATION
                        + " does not comply with regex: " + DATA_LAKE_ANNOTATIONS_PERIOD_PATTERN + ". "
                        + "For more details see documentation: " + DATA_LAKE_ANNOTATIONS_DOCUMENT_URL);
            }
            final String num = matcher.group(2);
            final String unit = matcher.group(3);
            if (num != null && unit != null) {
                final char u = unit.startsWith(" ") ? unit.charAt(1) : unit.charAt(0);
                final int n = Integer.parseInt(num);
                if (u == 'd' && n < 7) {
                    throw new InvalidEventTypeException(
                            "Annotation " + DATA_LAKE_RETENTION_PERIOD_ANNOTATION
                            + " value is too short (min: 7 days). "
                            + "For more details see documentation: " + DATA_LAKE_ANNOTATIONS_DOCUMENT_URL);
                }
                if (u == 'd' && n > 3650
                        || u == 'w' && n > 521
                        || u == 'm' && n > 120
                        || u == 'y' && n > 10) {
                    throw new InvalidEventTypeException(
                            "Annotation " + DATA_LAKE_RETENTION_PERIOD_ANNOTATION
                            + " value is too long (max: 10 years). "
                            + "For more details see documentation: " + DATA_LAKE_ANNOTATIONS_DOCUMENT_URL);
                }
            }
        }

        // Validation of @datalake.zalando.org/materialize-events is performed
        // only on new event-types or on event-types that have already migrated to using this new annotation.
        final var stricterCheck = (oldAnnotations == null
                || oldAnnotations.containsKey(DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION));
        if (stricterCheck && areDataLakeAnnotationsMandatory()) {
            if (materializeEvents == null) {
                throw new InvalidEventTypeException(
                        "Annotation " + DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION + " is required");
            }
        }
    }

    private boolean areDataLakeAnnotationsMandatory() {
        if (!featureToggleService.isFeatureEnabled(Feature.FORCE_DATA_LAKE_ANNOTATIONS)) {
            return false;
        }
        if (enforcedAuthSubjects.contains("*")) {
            return true;
        }

        final var subject = authorizationService.getSubject().map(Subject::getName).orElse("");
        return enforcedAuthSubjects.contains(subject);
    }
}
