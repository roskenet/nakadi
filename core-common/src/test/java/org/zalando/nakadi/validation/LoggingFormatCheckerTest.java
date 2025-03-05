package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LoggingFormatCheckerTest {

    final FormatValidator proxiedValidator = mock(FormatValidator.class);
    LoggingFormatChecker unit;

    @BeforeEach
    void setup() {
        when(proxiedValidator.validate(anyString())).thenReturn(Optional.of("input value is invalid"));
        when(proxiedValidator.formatName()).thenReturn("some-format");

        unit = new LoggingFormatChecker(proxiedValidator, "some-event-type");
    }

    @Test
    public void alwaysReturnsEmptyValidationResult() {
        final String someInput = "some-value";
        final Optional<String> result = unit.validate(someInput);

        Assertions.assertTrue(result.isEmpty());
        verify(proxiedValidator, times(1)).validate(someInput);
    }

    @Test
    public void formatNameMatchesFormatNameOfProxiedFormatValidator() {
        Assertions.assertEquals(proxiedValidator.formatName(), unit.formatName());
    }
}
