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
        final byte[] eventBytes = evalFilterRequest.getEvent().toString().getBytes();
        try {
            final Library library = new Library();
            final Criterion criterion = library.parseExpression(evalFilterRequest.getFilter());
            Function<EventsWrapper, Boolean> compiledPredicate = library.compilePredicate(criterion);
            final EventsWrapper predicateInput = library.singletonInput(eventBytes);
            final Boolean result = compiledPredicate.apply(predicateInput);

            final EvalFilterResponse response = new EvalFilterResponse();
            response.setResult(result);
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } catch (final SqlParserException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        } catch (final Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

}
