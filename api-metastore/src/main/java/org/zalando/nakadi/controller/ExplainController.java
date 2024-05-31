package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.model.EventTypeAuthExplainRequest;
import org.zalando.nakadi.model.EventTypeAuthExplainResult;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.exceptions.AuthorizationInvalidException;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.auth.AuthorizationResourceMapping;
import org.zalando.nakadi.service.validation.EventOwnerValidator;
import org.zalando.nakadi.service.validation.EventTypeAnnotationsValidator;

import javax.validation.Valid;
import java.util.Collections;
import java.util.Optional;

@RestController
@RequestMapping(value = "/explanations")
public class ExplainController {

    private final EventTypeAnnotationsValidator eventTypeAnnotationsValidator;
    private final AuthorizationValidator authorizationValidator;


    @Autowired
    public ExplainController(final EventTypeAnnotationsValidator eventTypeAnnotationsValidator,
                             final AuthorizationValidator authorizationValidator) {
        this.eventTypeAnnotationsValidator = eventTypeAnnotationsValidator;
        this.authorizationValidator = authorizationValidator;
    }
    @RequestMapping(value = "/event-type-auth", method = RequestMethod.POST)
    public ResponseEntity<EventTypeAuthExplainResult> explainEventTypeAuth(
            @Valid @RequestBody final EventTypeAuthExplainRequest eventTypeAuthExplainRequest)
            throws IllegalArgumentException, InvalidEventTypeException,
            AuthorizationInvalidException, UnableProcessException {

        final var newAnnotations = Optional.ofNullable(eventTypeAuthExplainRequest.getAnnotations())
                .orElseGet(Collections::emptyMap);
        eventTypeAnnotationsValidator.validateDataComplianceAnnotations(null, newAnnotations);

        final var authResource = eventTypeAuthExplainRequest.asEventTypeBase();
        EventOwnerValidator.validateEventOwnerSelector(authResource);

        final Resource eventTypeResource = AuthorizationResourceMapping.mapToResource(authResource);
        authorizationValidator.validateAuthorization(eventTypeResource);

        final var readersResult = authorizationValidator.explainReadAuthorization(eventTypeResource);
        return ResponseEntity.ok(new EventTypeAuthExplainResult(readersResult));
    }
}

