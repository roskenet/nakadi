package org.zalando.nakadi.service.auth;

import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;
import org.zalando.nakadi.domain.ValidatableAuthorization;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.view.EventOwnerSelector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class EventTypeAuthorization implements ValidatableAuthorization {
    @Nullable
    private final ResourceAuthorization authorization;
    @Nullable
    private final String aspdDataClassification;
    @Nullable
    private final EventOwnerSelector eventOwnerSelector;

    EventTypeAuthorization(
            @Nullable final ResourceAuthorization authorization,
            @Nullable final String aspdDataClassification,
            @Nullable final EventOwnerSelector eventOwnerSelector) {
        this.authorization = authorization;
        this.aspdDataClassification = aspdDataClassification;
        this.eventOwnerSelector = eventOwnerSelector;
    }

    @Override
    public Map<String, List<AuthorizationAttribute>> asMapValue() {
        if (authorization != null) {
            return authorization.asMapValue();
        }
        return null;
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) throws IllegalArgumentException {
        final Optional<List<AuthorizationAttribute>> attributes = authorization != null ?
                authorization.getAttributesForOperation(operation) : Optional.empty();
        if (operation == AuthorizationService.Operation.READ) {
            final var extendedAttributes = new ArrayList<AuthorizationAttribute>();
            attributes.ifPresent(extendedAttributes::addAll);
            // TODO: this is coupling in the core code with Auth plugin logic.
            if (aspdDataClassification != null) {
                extendedAttributes.add(
                        new ResourceAuthorizationAttribute("aspd-classification", aspdDataClassification));
            }
            if (eventOwnerSelector != null) {
                extendedAttributes.add(
                        new ResourceAuthorizationAttribute("event_owner_selector.name", eventOwnerSelector.getName()));
            }
            return Optional.of(extendedAttributes);
        }
        return attributes;
    }
}
