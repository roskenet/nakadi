package org.zalando.nakadi.exceptions.runtime;

import org.zalando.nakadi.domain.NakadiCursor;

public class FilterEvaluationException extends NakadiBaseException {

    private final NakadiCursor eventPosition;

    private final Exception cause;

    public FilterEvaluationException(final Exception cause, final NakadiCursor eventPosition) {
        this.eventPosition = eventPosition;
        this.cause = cause;
    }

    @Override
    public String getMessage() {
        return "Nakadi failed to evaluate the filter expression on the event at position " +
                eventPosition + ". Error: " + cause.getMessage();
    }

}
