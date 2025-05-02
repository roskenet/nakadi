package org.zalando.nakadi.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.aruha.nakadisql.api.criteria.Criterion;
import org.zalando.aruha.nakadisql.parser.nsql.SqlParserException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.TimelineException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;
import org.zalando.nakadi.filterexpression.FilterExpressionCompiler;
import org.zalando.nakadi.view.EvalFilterRequest;
import org.zalando.nakadi.view.EvalFilterResponse;
import org.zalando.nakadisqlexecutor.streams.EventsWrapper;

import java.util.Map;
import java.util.function.Function;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping(value = "/debug", produces = APPLICATION_JSON_VALUE)
public class DebugController {

    @RequestMapping(method = RequestMethod.POST, value = "/eval-filter")
    public ResponseEntity<?> testFilter(@RequestBody final EvalFilterRequest evalFilterRequest,
                                        final NativeWebRequest request) {
        try {
            final byte[] eventBytes = evalFilterRequest.getEvent().toString().getBytes();
            final FilterExpressionCompiler library = new FilterExpressionCompiler();
            final EventsWrapper predicateInput = library.singletonInput(eventBytes);

            final Criterion criterion;
            final Function<EventsWrapper, Boolean> compiledPredicate;
            final Boolean result;
            try {
                criterion = library.parseExpression(evalFilterRequest.getFilter());
            } catch (final SqlParserException e) {
                return reportError(HttpStatus.BAD_REQUEST,
                        "SQL_PARSER_ERROR", "filter expression could not be parsed", e);
            } catch (final Exception e) {
                return reportError(HttpStatus.BAD_REQUEST, "SQL_PARSER_UNEXPECTED_ERROR",
                        "an unexpected error occurred while parsing the filter expression", e);
            }
            try {
                compiledPredicate = library.compilePredicate(criterion);
            } catch (final Exception e) {
                return reportError(HttpStatus.BAD_REQUEST, "FILTER_COMPILATION_ERROR",
                        "filter expression could not be compiled", e);
            }
            try {
                result = compiledPredicate.apply(predicateInput);
            } catch (final Exception e) {
                return reportError(HttpStatus.BAD_REQUEST, "FILTER_EVALUATION_ERROR",
                        "evaluation of filter expression on the event resulted in an exception", e);
            }

            final EvalFilterResponse response = new EvalFilterResponse();
            response.setParsedFilter(criterion.toString());
            response.setResult(result);
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (final Exception e) {
            return reportError(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR",
                    "something unexpected went wrong", e);
        }
    }

    private ResponseEntity<?> reportError(
            final HttpStatus httpStatus, final String error, final String message, final Exception e) {
        // TODO use Problem class for error responses
        return ResponseEntity.status(httpStatus).body(Map.of(
                "error", error,
                "message", message,
                "caused_by", Map.of(
                        "exception", e.getClass().getName(),
                        "message", e.getMessage())
        ));
    }

}
