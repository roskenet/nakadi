package org.zalando.nakadi.plugin.auth.subject;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.auth.attribute.AuthorizationAttributeType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.zalando.nakadi.plugin.auth.ResourceType.EVENT_RESOURCE;

public class ExternalSubject extends Principal {
    private final Set<String> bpids;

    protected ExternalSubject(
            final String uid,
            final Supplier<Set<String>> retailerIdsSupplier,
            final Set<String> bpids) {
        super(uid, retailerIdsSupplier);
        this.bpids = bpids;
    }

    @Override
    public boolean isExternal() {
        return true;
    }

    @Override
    protected boolean isOperationAllowed(
            final String resourceType,
            final AuthorizationService.Operation operation, final Optional<List<AuthorizationAttribute>> attributes) {
        if (operation == AuthorizationService.Operation.VIEW) {
            if (!attributes.isPresent()) {
                return false;
            }
        }
        if (operation == AuthorizationService.Operation.READ && Objects.equals(EVENT_RESOURCE, resourceType)) {
            // This crutch is specifically designed to support externals use case.
            // The reason is that by default externals are not allowed to access data without any kind of auths defined.
            // That means, that they will lose access to all the data. In order to avoid it, we are allowing read
            // access to event that have no authz defined.
            if (attributes.get().isEmpty()) {
                // In case of event and when there are no authz in event by itself - just allow access (read)
                return true;
            }
        }
        for (final AuthorizationAttribute attribute : attributes.get()) {
            if (Objects.equals(attribute.getDataType(), AuthorizationAttributeType.AUTH_BUSINESS_PARTNER) &&
                    bpids.contains(attribute.getValue())) {
                return true;
            }
            if (isPerEventOperationAllowed(operation, attribute)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getName() {
        return getUid() + ":business_partners:(" + String.join(",", bpids) + ")";
    }

    @Override
    public Set<String> getBpids() {
        return bpids;
    }
}
