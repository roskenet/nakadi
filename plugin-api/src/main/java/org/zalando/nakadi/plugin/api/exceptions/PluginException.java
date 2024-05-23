package org.zalando.nakadi.plugin.api.exceptions;

public class PluginException extends RuntimeException {

    public PluginException(final String message) {
        super(message);
    }

    public PluginException(final String message, final Throwable e) {
        super(message, e);
    }

    public PluginException(final Throwable e) {
        super(e);
    }
}
