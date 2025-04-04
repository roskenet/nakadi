package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.Predicate;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LoggingFormatCheckerTest {

    final FormatValidator proxiedValidator = mock(FormatValidator.class);
    final FeatureTogglePredicate featureToggle = new FeatureTogglePredicate();
    LoggingFormatChecker unit;

    @BeforeEach
    void setup() {
        when(proxiedValidator.validate(anyString())).thenReturn(Optional.of("input value is invalid"));
        when(proxiedValidator.formatName()).thenReturn("some-format");

        unit = new LoggingFormatChecker(proxiedValidator, "some-event-type", featureToggle);
    }

    @Test
    public void returnsEmptyValidationResultIfToggleIsOff() {
        featureToggle.isFormatAsserted = false;
        final String someInput = "some-value";
        final Optional<String> result = unit.validate(someInput);

        Assertions.assertTrue(result.isEmpty());
        verify(proxiedValidator, times(1)).validate(someInput);
    }

    @Test
    public void returnsValidationResultIfToggleIsOn() {
        featureToggle.isFormatAsserted = true;
        final String someInput = "some-value";
        final Optional<String> result = unit.validate(someInput);

        Assertions.assertEquals(Optional.of("input value is invalid"), result);
        verify(proxiedValidator, times(1)).validate(someInput);
    }

    @Test
    public void formatNameMatchesFormatNameOfProxiedFormatValidator() {
        Assertions.assertEquals(proxiedValidator.formatName(), unit.formatName());
    }

    static class FeatureTogglePredicate implements Predicate<String> {
        boolean isFormatAsserted;

        @Override
        public boolean test(final String formatName) {
            return isFormatAsserted;
        }
    }
}
