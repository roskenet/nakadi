package org.zalando.nakadi.service.auth;

import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.ResourceImpl;
import org.zalando.nakadi.plugin.api.authz.EventTypeAuthz;
import org.zalando.nakadi.plugin.api.authz.Resource;

import javax.annotation.Nullable;

public class AuthorizationResourceMapping {
    public static final String DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION =
            "compliance.zalando.org/aspd-classification";

    public static Resource<EventTypeAuthz> mapToResource(final EventTypeBase eventType) {
        return new ResourceImpl<>(
                eventType.getName(),
                ResourceImpl.EVENT_TYPE_RESOURCE,
                new EventTypeAuthorization(
                        eventType.getAuthorization(),
                        getAspdDataClassification(eventType),
                        eventType.getEventOwnerSelector()),
                eventType);
    }

    private static @Nullable String getAspdDataClassification(final EventTypeBase eventType) {
        final var annotations = eventType.getAnnotations();
        if (annotations != null) {
            return annotations.get(DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION);
        }
        return null;
    }
}
