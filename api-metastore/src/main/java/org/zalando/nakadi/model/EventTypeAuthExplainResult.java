package org.zalando.nakadi.model;

import org.zalando.nakadi.plugin.api.authz.ExplainResourceResult;

import java.util.List;

public class EventTypeAuthExplainResult {
    private final Error error;
    private final List<ExplainResourceResult> readers;

    private EventTypeAuthExplainResult(final Error error, final List<ExplainResourceResult> readers) {
        this.error = error;
        this.readers = readers;
    }

    public static EventTypeAuthExplainResult fromExplainResult(final List<ExplainResourceResult> readers) {
        return new EventTypeAuthExplainResult(null, readers);
    }

    public static EventTypeAuthExplainResult fromErrorMessage(final String message) {
        return new EventTypeAuthExplainResult(new Error(message), null);
    }

    public Error getError() {
        return error;
    }

    public List<ExplainResourceResult> getReaders() {
        return readers;
    }

    @Override
    public String toString() {
        return "EventTypeAuthExplainResult{" +
                "error=" + error +
                ", readers=" + readers +
                '}';
    }

    public static class Error {
        private final String message;

        public Error(final String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return "Error{" +
                    "message='" + message + '\'' +
                    '}';
        }
    }
}