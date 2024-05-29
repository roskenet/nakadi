package org.zalando.nakadi.controller;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.annotations.validation.AnnotationKey;
import org.zalando.nakadi.annotations.validation.AnnotationValue;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.exceptions.runtime.ValidationException;
import org.zalando.nakadi.plugin.api.authz.ExplainResourceResult;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.auth.AuthorizationResourceMapping;
import org.zalando.nakadi.service.validation.EventOwnerValidator;
import org.zalando.nakadi.service.validation.EventTypeAnnotationsValidator;
import org.zalando.nakadi.view.EventOwnerSelector;

import javax.annotation.Nullable;
import javax.validation.Valid;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping(value = "/explanations")
public class ExplainController {

    private EventTypeAnnotationsValidator eventTypeAnnotationsValidator;
    private final AuthorizationValidator authorizationValidator;


    @Autowired
    public ExplainController(final EventTypeAnnotationsValidator eventTypeAnnotationsValidator,
                             final AuthorizationValidator authorizationValidator) {
        this.eventTypeAnnotationsValidator = eventTypeAnnotationsValidator;
        this.authorizationValidator = authorizationValidator;
    }
    @RequestMapping(value = "/event-type-auth", method = RequestMethod.POST)
    public ResponseEntity<EventTypeAuthExplainResult> explainEventTypeAuth(
            @Valid @RequestBody final EventTypeAuthExplainRequest eventTypeAuthExplainRequest,
            final Errors errors) {
        if (errors.hasErrors()) {
            throw new ValidationException(errors);
        }


        final var newAnnotations = Optional.ofNullable(eventTypeAuthExplainRequest.getAnnotations())
                .orElseGet(Collections::emptyMap);
        eventTypeAnnotationsValidator.validateDataComplianceAnnotations(null, newAnnotations);

        final var authResource = eventTypeAuthExplainRequest.asEventTypeBase();
        EventOwnerValidator.validateEventOwnerSelector(authResource);

        final Resource eventTypeResource = AuthorizationResourceMapping.mapToResource(authResource);
        authorizationValidator.validateAuthorization(eventTypeResource);

        final var readersResult = authorizationValidator.explainReadAuthorization(eventTypeResource);
        return ResponseEntity.ok(new EventTypeAuthExplainResult(eventTypeAuthExplainRequest, readersResult));
    }

    public static class EventTypeAuthExplainRequest {

        public EventTypeAuthExplainRequest(final Map<String,String> annotations,
                                           final EventOwnerSelector eventOwnerSelector,
                                           final ResourceAuthorization authorization) {
            this.annotations = annotations;
            this.eventOwnerSelector = eventOwnerSelector;
            this.authorization = authorization;
        }

        public EventTypeAuthExplainRequest(final EventTypeAuthExplainRequest eventTypeAuthExplainRequest) {
            this.annotations = eventTypeAuthExplainRequest.getAnnotations();
            this.eventOwnerSelector = eventTypeAuthExplainRequest.getEventOwnerSelector();
            this.authorization = eventTypeAuthExplainRequest.getAuthorization();
        }

        @Valid
        @Nullable
        private final Map<
                @AnnotationKey String,
                @AnnotationValue String> annotations;

        @Valid
        @Nullable
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final EventOwnerSelector eventOwnerSelector;

        @Valid
        private final ResourceAuthorization authorization;

        @Nullable
        public Map<String, String> getAnnotations() {
            return annotations;
        }

        @Nullable
        public EventOwnerSelector getEventOwnerSelector() {
            return eventOwnerSelector;
        }

        public ResourceAuthorization getAuthorization() {
            return authorization;
        }

        public EventTypeBase asEventTypeBase() {
            final var result = new EventTypeBase();
            result.setName("explain-auth-" + UUID.randomUUID());
            result.setAnnotations(annotations == null? Collections.emptyMap(): annotations);
            result.setAuthorization(authorization);
            result.setEventOwnerSelector(eventOwnerSelector);
            return result;
        }
    }

    public static class EventTypeAuthExplainResult {

        private List<ExplainResourceResult> readers;

        public EventTypeAuthExplainResult(final EventTypeAuthExplainRequest request,
                                          final List<ExplainResourceResult> readers) {
            this.readers = readers;
        }

        public List<ExplainResourceResult> getReaders() {
            return readers;
        }

        public void setReaders(final List<ExplainResourceResult> readers) {
            this.readers = readers;
        }

        @Override
        public String toString() {
            return "EventTypeAuthExplainResult{" +
                    "readers=" + readers +
                    '}';
        }
    }
}
