package org.zalando.nakadi.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.service.auth.AuthorizationResourceMapping;
import org.zalando.nakadi.service.validation.EventOwnerValidator;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.EventOwnerSelector;

import javax.annotation.Nullable;
import java.util.stream.Stream;

public class EventTypeServiceAntitrustComplianceSettingsTest {

    @ParameterizedTest
    @MethodSource("getValidParameters")
    void testValidParameters(@Nullable final String aspdDataClassification, @Nullable final EventOwnerSelector eos) {
        final var et = buildEventType(aspdDataClassification, eos);
        EventOwnerValidator.validateEventOwnerSelector(et);
    }

    public static Stream<Arguments> getValidParameters() {
        return Stream.of(
                // EOS with 'metadata' is supported only without value
                Arguments.of(
                        null,
                        new EventOwnerSelector(EventOwnerSelector.Type.METADATA, "*any_name*", null)
                ),
                // Old-style EOS without ASPD data classification set: no restrictions on the name
                Arguments.of(
                        null,
                        new EventOwnerSelector(EventOwnerSelector.Type.PATH, "*any_name*", "*any_value*")
                ),
                // ASPD classification is "none", EOS is not allowed.
                Arguments.of(
                        "none",
                        null
                ),
                // ASPD classification is "aspd", EOS (with merchant_id) is optional.
                Arguments.of(
                        "aspd",
                        null
                ),
                // ASPD classification is "mcf-aspd", EOS (with retailer_id or merchant_id) is optional.
                Arguments.of(
                        "mcf-aspd",
                        null
                ),
                Arguments.of(
                        "mcf-aspd",
                        new EventOwnerSelector(EventOwnerSelector.Type.PATH, "retailer_id", "*any_value*")
                )
        );
    }

    @ParameterizedTest
    @MethodSource("getInvalidParameters")
    void testInvalidParameters(
            @Nullable final String aspdDataClassification,
            @Nullable final EventOwnerSelector eos,
            final Class<Throwable> expected) {

        final var et = buildEventType(aspdDataClassification, eos);
        Assertions.assertThrows(
                expected,
                () -> EventOwnerValidator.validateEventOwnerSelector(et));
    }

    public static Stream<Arguments> getInvalidParameters() {
        return Stream.of(
                // EOS with 'metadata' is supported only without value
                Arguments.of(
                        null,
                        new EventOwnerSelector(EventOwnerSelector.Type.METADATA, "*any_name*", "*any_value*"),
                        InvalidEventTypeException.class
                ),
                // Not supported ASPD classification - internal error, the actual user input should be handled earlier
                Arguments.of(
                        "*something*",
                        new EventOwnerSelector(EventOwnerSelector.Type.PATH, "*any_name*", "*any_value*"),
                        IllegalStateException.class
                ),
                // ASPD classification is "none", EOS is not allowed.
                Arguments.of(
                        "none",
                        new EventOwnerSelector(EventOwnerSelector.Type.PATH, "*any_name*", "*any_value*"),
                        InvalidEventTypeException.class
                ),
                // ASPD classification is "aspd": EOS is optional (might be supported in the future).
                Arguments.of(
                        "aspd",
                        new EventOwnerSelector(EventOwnerSelector.Type.PATH, "*some_name*", "*any_value*"),
                        InvalidEventTypeException.class
                ),
                Arguments.of(
                        "aspd",
                        new EventOwnerSelector(EventOwnerSelector.Type.PATH, "retailer_id", "*any_value*"),
                        InvalidEventTypeException.class
                ),
                // Not supported yet -- reject for now
                Arguments.of(
                        "aspd",
                        new EventOwnerSelector(EventOwnerSelector.Type.PATH, "merchant_id", "*any_value*"),
                        InvalidEventTypeException.class
                ),
                // ASPD classification is "mcf-aspd", EOS is optional.
                Arguments.of(
                        "mcf-aspd",
                        new EventOwnerSelector(EventOwnerSelector.Type.PATH, "*some_name*", "*any_value*"),
                        InvalidEventTypeException.class
                ),
                // Not supported yet -- reject for now
                Arguments.of(
                        "mcf-aspd",
                        new EventOwnerSelector(EventOwnerSelector.Type.PATH, "merchant_id", "any_value"),
                        InvalidEventTypeException.class
                )
        );
    }

    private static EventType buildEventType(
            @Nullable final String aspdDataClassification,
            @Nullable final EventOwnerSelector eos) {
        final EventType et = TestUtils.buildDefaultEventType();
        if (aspdDataClassification != null) {
            et.getAnnotations().put(
                    AuthorizationResourceMapping.DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION,
                    aspdDataClassification);
        }
        et.setEventOwnerSelector(eos);
        return et;
    }
}
