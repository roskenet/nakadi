package org.zalando.nakadi.plugin.api.exceptions;

public class OperationOnResourceNotPermittedException extends RuntimeException {

    public OperationOnResourceNotPermittedException(final String message) {
        super(message);
    }
}
