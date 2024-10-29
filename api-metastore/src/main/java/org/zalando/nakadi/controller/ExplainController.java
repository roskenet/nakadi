package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.model.EventTypeAuthExplainRequest;
import org.zalando.nakadi.model.EventTypeAuthExplainResult;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.auth.AuthorizationResourceMapping;
import org.zalando.nakadi.service.validation.EventOwnerValidator;
import org.zalando.nakadi.service.validation.EventTypeAnnotationsValidator;

import javax.validation.Valid;

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
            @Valid @RequestBody final EventTypeAuthExplainRequest eventTypeAuthExplainRequest) {

        final var authResource = eventTypeAuthExplainRequest.asEventTypeBase();
        try {
            eventTypeAnnotationsValidator.validateDataComplianceAnnotations(
                    null,
                    authResource.getAnnotations(),
                    null
            );
            EventOwnerValidator.validateEventOwnerSelector(authResource);
        } catch (InvalidEventTypeException ex) {
            return ResponseEntity.ok(EventTypeAuthExplainResult.fromErrorMessage(ex.getMessage()));
        }

        final var eventTypeResource = AuthorizationResourceMapping.mapToResource(authResource);
        try {
            authorizationValidator.validateAuthorization(eventTypeResource);
        } catch (RuntimeException ex) {
            if (ex instanceof PluginException) {
                throw ex;
            }
            return ResponseEntity.ok(EventTypeAuthExplainResult.fromErrorMessage(ex.getMessage()));
        }

        final var result = authorizationValidator.explainAuthorization(eventTypeResource);
        return ResponseEntity.ok(EventTypeAuthExplainResult.fromExplainResult(result));
    }
}

