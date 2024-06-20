package org.zalando.nakadi.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.zalando.nakadi.annotations.validation.AnnotationKey;
import org.zalando.nakadi.annotations.validation.AnnotationValue;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.view.EventOwnerSelector;

import javax.annotation.Nullable;
import javax.validation.Valid;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class EventTypeAuthExplainRequest {

    public EventTypeAuthExplainRequest() {
    }

    public EventTypeAuthExplainRequest(final Map<String, String> annotations,
                                       final EventOwnerSelector eventOwnerSelector,
                                       final ResourceAuthorization authorization) {
        this.annotations = annotations;
        this.eventOwnerSelector = eventOwnerSelector;
        this.authorization = authorization;
    }

    @Valid
    @Nullable
    private Map<
            @AnnotationKey String,
            @AnnotationValue String> annotations;

    @Valid
    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private EventOwnerSelector eventOwnerSelector;

    @Valid
    private ResourceAuthorization authorization;

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
        result.setAnnotations(annotations == null ? Collections.emptyMap() : annotations);
        result.setAuthorization(authorization);
        result.setEventOwnerSelector(eventOwnerSelector);
        return result;
    }
}
