package org.zalando.nakadi.service.auth;

import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.ResourceAuthorizationProperty;
import org.zalando.nakadi.domain.ResourceImpl;
import org.zalando.nakadi.plugin.api.authz.AuthorizationProperty;
import org.zalando.nakadi.plugin.api.authz.EventTypeAuthz;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.authz.ResourceType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AuthorizationResourceMapping {
    public static final String DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION =
            "compliance.zalando.org/aspd-classification";

    public static Resource<EventTypeAuthz> mapToResource(final EventTypeBase eventType) {
        return new ResourceImpl<>(
                eventType.getName(),
                ResourceType.EVENT_TYPE_RESOURCE,
                eventType.getAuthorization(),
                eventType,
                getProperties(eventType));
    }

    private static List<AuthorizationProperty> getProperties(final EventTypeBase eventType) {
        final List<AuthorizationProperty> properties = new ArrayList<>();
        // TODO: this is coupling in the core code with Auth plugin logic.
        final var aspdDataClassification = getAspdDataClassification(eventType);
        if (aspdDataClassification != null) {
            properties.add(new ResourceAuthorizationProperty(
                    "aspd-classification",
                    aspdDataClassification));
        }
        final var eventOwnerSelector = eventType.getEventOwnerSelector();
        if (eventOwnerSelector != null) {
            properties.add(new ResourceAuthorizationProperty(
                    "event_owner_selector.name",
                    eventOwnerSelector.getName()));
        }
        return Collections.unmodifiableList(properties);
    }

    private static @Nullable String getAspdDataClassification(final EventTypeBase eventType) {
        final var annotations = eventType.getAnnotations();
        if (annotations != null) {
            return annotations.get(DATA_COMPLIANCE_ASPD_CLASSIFICATION_ANNOTATION);
        }
        return null;
    }
}
