package org.zalando.nakadi.exceptions.runtime;

public class InvalidFilterException extends NakadiBaseException {
    private final String filter;
    private final String message;

    public InvalidFilterException(final String filter, final String message) {
        this.filter = filter;
        this.message = message;
    }

    @Override
    public String getMessage() {
        return "Nakadi could not use the supplied filter: (" + filter +
                "). Error: " + message;
    }

}
