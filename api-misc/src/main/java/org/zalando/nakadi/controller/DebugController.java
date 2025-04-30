package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.aruha.nakadisql.api.criteria.Criterion;
import org.zalando.aruha.nakadisql.parser.nsql.SqlParserException;
import org.zalando.nakadi.exceptions.runtime.*;
import org.zalando.nakadi.filterexpression.Library;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.view.EvalFilterRequest;
import org.zalando.nakadi.view.EvalFilterResponse;
import org.zalando.nakadi.view.TimelineRequest;
import org.zalando.nakadisqlexecutor.streams.EventsWrapper;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping(value = "/debug", produces = APPLICATION_JSON_VALUE)
public class DebugController {

    @RequestMapping(method = RequestMethod.POST, value = "/eval-filter")
    public ResponseEntity<?> testFilter(@RequestBody final EvalFilterRequest evalFilterRequest,
                                        final NativeWebRequest request)
            throws AccessDeniedException, TimelineException, TopicRepositoryException, InconsistentStateException,
            RepositoryProblemException {
        try {
            final byte[] eventBytes = evalFilterRequest.getEvent().toString().getBytes();
            final Library library = new Library();
            final EventsWrapper predicateInput = library.singletonInput(eventBytes);

            // TODO use Problem class for error responses
            Criterion criterion;
            Function<EventsWrapper, Boolean> compiledPredicate;
            final Boolean result;
            try {
                criterion = library.parseExpression(evalFilterRequest.getFilter());
            } catch (final SqlParserException e) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                        "error", "SQL_PARSER_ERROR",
                        "message", "filter expression could not be parsed",
                        "caused_by", Map.of(
                                "exception", e.getClass().getName(),
                                "message", e.getMessage())
                ));
            } catch (final Exception e) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                        "error", "SQL_PARSER_UNEXPECTED_ERROR",
                        "message", "an unexpected error occurred while parsing the filter expression",
                        "caused_by", Map.of(
                                "exception", e.getClass().getName(),
                                "message", e.getMessage())
                ));
            }
            try {
                compiledPredicate = library.compilePredicate(criterion);
            } catch (final Exception e) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                        "error", "FILTER_COMPILATION_ERROR",
                        "message", "filter expression could not be compiled",
                        "caused_by", Map.of(
                                "exception", e.getClass().getName(),
                                "message", e.getMessage())
                ));
            }
            try {
                result = compiledPredicate.apply(predicateInput);
            } catch (final Exception e) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                        "error", "FILTER_EVALUATION_ERROR",
                        "message", "evaluation of filter expression on the event resulted in an exception",
                        "caused_by", Map.of(
                                "exception", e.getClass().getName(),
                                "message", e.getMessage())
                ));
            }

            final EvalFilterResponse response = new EvalFilterResponse();
            response.setParsedFilter(criterion.toString());
            response.setResult(result);
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (final Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "error", "INTERNAL_SERVER_ERROR",
                    "message", "something unexpected went wrong",
                    "caused_by", Map.of(
                            "exception", e.getClass().getName(),
                            "message", e.getMessage())
            ));
        }
    }

}
