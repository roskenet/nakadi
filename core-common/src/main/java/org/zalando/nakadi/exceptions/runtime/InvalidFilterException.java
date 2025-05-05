package org.zalando.nakadi.exceptions.runtime;

public class InvalidFilterException extends NakadiBaseException {

    @Override
    public String getMessage() {
        return "The filter is invalid. Please check the filter syntax and try again.";
    }

}
