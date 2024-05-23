package org.zalando.nakadi.plugin.api.exceptions;

public class AuthorizationInvalidException extends RuntimeException {

    public AuthorizationInvalidException(final String message) {
        super(message);
    }
}
