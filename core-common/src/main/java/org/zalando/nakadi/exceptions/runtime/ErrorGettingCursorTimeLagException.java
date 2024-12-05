package org.zalando.nakadi.exceptions.runtime;

import org.zalando.nakadi.domain.NakadiCursor;

import java.util.Optional;

public class ErrorGettingCursorTimeLagException extends NakadiBaseException {

    public ErrorGettingCursorTimeLagException(
            final NakadiCursor failedCursor,
            final Optional<NakadiCursor> lastCursor,
            final Throwable cause) {
        super("Could not determine time lag: failedCursor=" + failedCursor + "; lastCursor=" + lastCursor, cause);
    }
}
