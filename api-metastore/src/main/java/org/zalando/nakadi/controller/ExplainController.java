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
    public ExplainController(EventTypeAnnotationsValidator eventTypeAnnotationsValidator,
                             final AuthorizationValidator authorizationValidator) {
        this.eventTypeAnnotationsValidator = eventTypeAnnotationsValidator;
        this.authorizationValidator = authorizationValidator;
    }
    @RequestMapping(value = "/event-type-auth", method = RequestMethod.POST)
    public ResponseEntity<EventTypeAuthExplainResult> explainEventTypeAuth(@Valid @RequestBody final ExplainController.EventTypeAuthExplainRequest eventTypeAuthExplainRequest,
                                                                           final Errors errors) {
        if (errors.hasErrors()) {
            throw new ValidationException(errors);
        }

        /*
            TODO: validation
            1. check for required annotation for mcf classification -- done
            2. Check if user/team/application is valid by appropriate api  --done
            3. if classification is none then skip --done
            (only retailer is supported)
            4. If classification is aspd then throw error (maybe not needed) --done
            5. If classification is mcf-aspd then only retailer should be present in EOS --done

            TODO: resolve
            6. If readers.attribute.type is team then resolve it to users --done

            TODO: resolve
            7. For each user hit OPA and get retailers --done

            TODO: form result
            8. Create result, put in team ref if present --done
            9. Put in human readable result probably from constants for different combinations? --done
         */

        //1.
        final var newAnnotations = Optional.ofNullable(eventTypeAuthExplainRequest.getAnnotations())
                .orElseGet(Collections::emptyMap);
        eventTypeAnnotationsValidator.validateDataComplianceAnnotations(null, newAnnotations);

        final var authResource = eventTypeAuthExplainRequest.asEventTypeBase();
        //3 and 5.
        EventOwnerValidator.validateEventOwnerSelector(authResource);
        //2
        final Resource eventTypeResource = AuthorizationResourceMapping.mapToResource(authResource);
        authorizationValidator.validateAuthorization(eventTypeResource);
        //6, 7, 8, 9
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

    public static class EventTypeAuthExplainResult extends EventTypeAuthExplainRequest {

        private List<ExplainResourceResult> readers;

        public EventTypeAuthExplainResult(final EventTypeAuthExplainRequest request,
                                          final List<ExplainResourceResult> readers) {
            super(request);
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
                    ", annotations=" + getAnnotations() +
                    ", eventOwnerSelector=" + getEventOwnerSelector() +
                    ", authorization=" + getAuthorization() +
                    '}';
        }
    }
}
