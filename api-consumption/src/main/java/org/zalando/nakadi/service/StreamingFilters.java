package org.zalando.nakadi.service;

import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.TestDataFilter;
import org.zalando.nakadi.filterexpression.FilterExpressionCompiler;
import org.zalando.nakadisqlexecutor.streams.EventsWrapper;

import java.util.function.Function;

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

    public static boolean shouldEventBeFilteredBecauseOfFilter(
            final Function<EventsWrapper, Boolean> filterPredicate,
            final ConsumedEvent event) {
        if (null == filterPredicate) {
            return false;
        } else {
            final byte[] eventData = event.getEvent();
            final EventsWrapper eventsWrapper = FilterExpressionCompiler.singletonInput(eventData);
            return !filterPredicate.apply(eventsWrapper);
        }
    }

}
