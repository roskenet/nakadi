package org.zalando.nakadi.controller.advice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.ExplainController;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.plugin.api.exceptions.AuthorizationInvalidException;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.AdviceTrait;

import javax.annotation.Priority;

import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@Priority(10)
@ControllerAdvice(assignableTypes = ExplainController.class)
public class ExplainExceptionHandler implements AdviceTrait {

    @ExceptionHandler({IllegalArgumentException.class,
            InvalidEventTypeException.class,
            AuthorizationInvalidException.class,
            UnableProcessException.class})
    public ResponseEntity<Problem> handleEventTypeDeletionException(final RuntimeException exception,
                                                                    final NativeWebRequest request) {
        AdviceTrait.LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, exception.getMessage()), request);
    }
}
