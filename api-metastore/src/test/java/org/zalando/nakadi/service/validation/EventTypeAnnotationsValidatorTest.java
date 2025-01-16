package org.zalando.nakadi.service.validation;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zalando.nakadi.config.OptInTeamsConfig;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.auth.AuthorizationResourceMapping;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class EventTypeAnnotationsValidatorTest {
    private static final String A_TEST_APPLICATION = "baz";

    private FeatureToggleService featureToggleService;
    private AuthorizationService authorizationService;
    private OptInTeamsConfig optInTeamsConfig;
    private ApplicationService applicationService;
    private EventTypeAnnotationsValidator validator;
    private static final String OWNING_APPLICATION_1 = "stups_nakadi";
    private static final String OWNING_APPLICATION_2 = "stups_erases";

    @BeforeEach
    public void setUp() {
        featureToggleService = mock(FeatureToggleService.class);
        authorizationService = mock(AuthorizationService.class);
        optInTeamsConfig = mock(OptInTeamsConfig.class);
        applicationService = mock(ApplicationService.class);
        validator = new EventTypeAnnotationsValidator(
                featureToggleService,
                authorizationService,
                optInTeamsConfig,
                applicationService,
                Collections.singletonList(A_TEST_APPLICATION));

        final String teamId1 = "50061346";
        final String teamId2 = "9876543";

        when(applicationService.getOwningTeamId(OWNING_APPLICATION_1)).thenReturn(Optional.of(teamId1));
        when(applicationService.getOwningTeamId(OWNING_APPLICATION_2)).thenReturn(Optional.of(teamId2));
        when(optInTeamsConfig.getOptInTeams()).thenReturn(Set.of(teamId1));
    }

    @ParameterizedTest
    @MethodSource("getValidDataComplianceAnnotations")
    public void testValidDataComplianceAnnotations(
            @Nullable final String annotationValue,
            @Nullable final String owningApplication) {
        final Map<String, String> annotations =
                Map.of(AuthorizationResourceMapping.DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION, annotationValue);
        validator.validateDataComplianceAnnotations(null, annotations, owningApplication);
    }

    private static Stream<Arguments> getValidDataComplianceAnnotations() {
        return Stream.of(
                Arguments.of("none", OWNING_APPLICATION_1),
                Arguments.of("aspd", OWNING_APPLICATION_1),
                Arguments.of("mcf-aspd", OWNING_APPLICATION_1),
                Arguments.of("none", null),
                Arguments.of("aspd", null),
                Arguments.of("mcf-aspd", null)
        );
    }

    @ParameterizedTest
    @MethodSource("getInvalidDataComplianceAnnotations")
    public void testInvalidDataComplianceAnnotations(
            @Nullable final String annotationValue,
            @Nullable final String owningApplication
    ) {
        final Map<String, String> annotations = annotationValue != null ?
                Map.of(AuthorizationResourceMapping.DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION, annotationValue) :
                Collections.emptyMap();

        final var exception = assertThrows(
                InvalidEventTypeException.class,
                () -> validator.validateDataComplianceAnnotations(null, annotations, owningApplication));
        Assertions.assertThat(exception.getMessage())
                .contains(AuthorizationResourceMapping.DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION);
    }

    private static Stream<Arguments> getInvalidDataComplianceAnnotations() {
        return Stream.of(
                Arguments.of("NONE", OWNING_APPLICATION_2),
                Arguments.of("ASPD", OWNING_APPLICATION_1),
                Arguments.of("MCF-ASPD", null),
                Arguments.of("mcf_aspd", OWNING_APPLICATION_2),
                Arguments.of(null, OWNING_APPLICATION_1)
        );
    }

    @Test
    public void testNullValueForDataComplianceAnnotationsWhenOldAnnotationNull() {
        final Map<String, String> annotations = new HashMap<>();
        annotations.put(AuthorizationResourceMapping.DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION, null);

        final var exception = assertThrows(
                InvalidEventTypeException.class,
                () -> validator.validateDataComplianceAnnotations(null, annotations, OWNING_APPLICATION_2));
        Assertions.assertThat(exception.getMessage())
                .isEqualTo("Annotation compliance.zalando.org/aspd-classification cannot have null value");
    }

    @Test
    public void testEmptyDataComplianceAnnotations() {
        final Map<String, String> annotations = Map.of();
        assertDoesNotThrow(
                () -> validator.validateDataComplianceAnnotations(null, annotations, OWNING_APPLICATION_2)
        );
    }

    @Test
    public void testDataComplianceAnnotationCannotBeRemovedOnUpdate() {
        final var exception = assertThrows(
                InvalidEventTypeException.class,
                () -> validator.validateDataComplianceAnnotations(
                        Map.of(AuthorizationResourceMapping.DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION, "a-value"),
                        Collections.emptyMap(),
                        OWNING_APPLICATION_1));
        Assertions.assertThat(exception.getMessage())
                .contains(AuthorizationResourceMapping.DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION, " is required");
    }

    @Test
    public void testDataComplianceAnnotationsOnlyWhenNewEventTypeCreated() {
        final Map<String, String> annotations =
                Map.of(AuthorizationResourceMapping.DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION, "aspd");
        validator.validateDataComplianceAnnotations(annotations, annotations, OWNING_APPLICATION_1);

        verifyNoInteractions(applicationService, optInTeamsConfig);
    }

    @ParameterizedTest
    @MethodSource("getValidDataLakeAnnotations")
    public void testValidDataLakeAnnotations(final Map<String, String> annotations) {
        validator.validateDataLakeAnnotations(null, annotations);
    }

    public static Stream<Map<String, String>> getValidDataLakeAnnotations() {
        return Stream.of(
                Stream.of("off", "on")
                        .map(materialization -> Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION, materialization,
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION, "1m",
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_REASON_ANNOTATION, "for testing"
                        )),
                Stream.of(
                        "unlimited",
                        "12 days",
                        "3650 days",
                        "120 months",
                        "1 month",
                        "10 years",
                        "25d",
                        "1m",
                        "2y",
                        "1 year"
                )
                        .map(retentionPeriod -> Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION, retentionPeriod,
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_REASON_ANNOTATION, "I need my data"
                        )),
                Stream.of(
                        Map.of("some-annotation", "some-value")
                )
        ).flatMap(Function.identity());
    }

    @ParameterizedTest
    @MethodSource("getInvalidDataLakeAnnotations")
    public void testInvalidDataLakeAnnotations(final Map<String, String> annotations, final String[] errorMessages) {
        final var exception = assertThrows(
                InvalidEventTypeException.class,
                () -> validator.validateDataLakeAnnotations(null, annotations));
        Assertions.assertThat(exception.getMessage()).contains(errorMessages);
    }

    public static Stream<Arguments> getInvalidDataLakeAnnotations() {
        return Stream.of(
                Arguments.of(
                        // Invalid materialization value
                        Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION, "true"
                        ),
                        new String[] {
                                EventTypeAnnotationsValidator.DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION,
                        }
                ),
                Arguments.of(
                        // Materialization is on without retention period
                        Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION, "on"
                        ),
                        new String[] {
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION,
                        }
                ),
                Arguments.of(
                        // Retention period without retention reason
                        Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION, "1 day"
                        ),
                        new String[] {
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_REASON_ANNOTATION,
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION,
                        }
                ),
                Arguments.of(
                        // Wrong retention period value
                        Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION, "1 airplane",
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_REASON_ANNOTATION, "I need my data"
                        ),
                        new String[] {
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION,
                                "https://docs.google.com/document/d/1-SwwpwUqauc_pXu-743YA1gO8l5_R_Gf4nbYml1ySiI",
                        }
                )
        );
    }

    @Test
    public void whenDataLakeAnnotationsEnforcedThenMaterializationIsRequired() {
        when(featureToggleService.isFeatureEnabled(Feature.FORCE_DATA_LAKE_ANNOTATIONS)).thenReturn(true);
        when(authorizationService.getSubject()).thenReturn(Optional.of(() -> A_TEST_APPLICATION));

        final var exception = assertThrows(
                InvalidEventTypeException.class,
                () -> validator.validateDataLakeAnnotations(null, Collections.emptyMap()));
        Assertions.assertThat(exception.getMessage())
                .contains(EventTypeAnnotationsValidator.DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION);
    }

    @Test
    public void testUpdateOfOldVersionWithoutDataLakeAnnotations() {
        when(featureToggleService.isFeatureEnabled(Feature.FORCE_DATA_LAKE_ANNOTATIONS)).thenReturn(true);
        when(authorizationService.getSubject()).thenReturn(Optional.of(() -> A_TEST_APPLICATION));

        validator.validateDataLakeAnnotations(Collections.emptyMap(), Collections.emptyMap());
    }
}
