package org.zalando.nakadi.service;

import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.TestDataFilter;

public class StreamingFilters {


    public static boolean shouldEventBeFilteredBecauseOfTestProjectId(
            final TestDataFilter testDataFilter,
            final ConsumedEvent event) {
        // If the user requested only LIVE events we need to discard any test events.
        // If the user requested only TEST events we need to discard any non-test events.
        final boolean isTestEvent = event.getTestProjectIdHeader().isPresent();
        if ((testDataFilter == TestDataFilter.LIVE && isTestEvent) ||
                (testDataFilter == TestDataFilter.TEST && !isTestEvent)) {
            return true;
        }
        return false;
    }

}
