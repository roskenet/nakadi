package org.zalando.nakadi.service;

import org.zalando.aruha.nakadisql.api.criteria.Criterion;
import org.zalando.aruha.nakadisql.parser.nsql.SqlParserException;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.TestDataFilter;
import org.zalando.nakadi.exceptions.runtime.FilterEvaluationException;
import org.zalando.nakadi.exceptions.runtime.InvalidFilterException;
import org.zalando.nakadi.exceptions.runtime.InvalidFilterLangException;
import org.zalando.nakadi.exceptions.runtime.MissingFilterException;
import org.zalando.nakadi.exceptions.runtime.MissingFilterLangException;
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
            try {
                final byte[] eventData = event.getEvent();
                final EventsWrapper eventsWrapper = FilterExpressionCompiler.singletonInput(eventData);
                return !filterPredicate.apply(eventsWrapper);
            } catch (Exception e) {
                throw new FilterEvaluationException(e, event.getPosition());
            }
        }
    }

    public static Function<EventsWrapper, Boolean> filterExpressionToPredicate(final String filter, final String lang) {
        if (filter == null) {
            if (lang != null) {
                throw new MissingFilterException();
            }
            return null;
        }
        if (filter.trim().isEmpty()) {
            throw new InvalidFilterException(filter, "Filter cannot be empty");
        }
        if (lang == null) {
            throw new MissingFilterLangException();
        }
        if (!lang.equals("sql_v1")) {
            throw new InvalidFilterLangException(lang);
        }
        try {
            final Criterion criterion = new FilterExpressionCompiler().parseExpression(filter);
            return new FilterExpressionCompiler()
                    .compilePredicate(criterion);
        } catch (SqlParserException e) {
            throw new InvalidFilterException(filter, "Could not parse SQL expression.");
        } catch (Exception e) {
            throw new InvalidFilterException(filter, "Could not compile SQL expression.");
        }
    }

}
