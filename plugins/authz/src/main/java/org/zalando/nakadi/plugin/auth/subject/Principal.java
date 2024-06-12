package org.zalando.nakadi.plugin.auth.subject;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationProperty;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;
import org.zalando.nakadi.plugin.auth.attribute.AuthorizationAttributeType;
import org.zalando.nakadi.plugin.auth.property.AuthorizationPropertyType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.zalando.nakadi.plugin.auth.ResourceType.EVENT_TYPE_RESOURCE;

public abstract class Principal implements Subject {
    private final String uid;

    private final Supplier<Set<String>> retailerIdsSupplier;
    private Set<String> cachedRetailerIds;

    protected Principal(final String uid, final Supplier<Set<String>> retailerIdsSupplier) {
        this.uid = uid;
        this.retailerIdsSupplier = retailerIdsSupplier;
    }

    public String getUid() {
        return uid;
    }

    public abstract boolean isExternal();

    public boolean isAuthorized(
            final String resourceType,
            final AuthorizationService.Operation operation,
            final Optional<List<AuthorizationAttribute>> attributes,
            final List<AuthorizationProperty> properties) {
        if (!isOperationAllowed(resourceType, operation, attributes)) {
            return false;
        }
        if (operation == AuthorizationService.Operation.READ && Objects.equals(EVENT_TYPE_RESOURCE, resourceType)) {
            if (!isEventTypeAccessAllowedByDataAccessPolicy(properties)) {
                return false;
            }
        }
        return true;
    }

    protected abstract boolean isOperationAllowed(
            String resourceType,
            AuthorizationService.Operation operation,
            Optional<List<AuthorizationAttribute>> attributes);

    private boolean isEventTypeAccessAllowedByDataAccessPolicy(
            final List<AuthorizationProperty> properties) {
        if (properties.isEmpty()) {
            return true;
        }

        final var aspdDataClassification = findAuthorizationProperty(
                properties, AuthorizationPropertyType.ASPD_DATA_CLASSIFICATION);
        if (aspdDataClassification.isEmpty()) {
            return true;
        }
        switch (aspdDataClassification.get()) {
            case "none":
                return true;
            case "aspd":
                if (getRetailerIdsToRead().isEmpty()) {
                    return false;
                }
                return true;
            case "mcf-aspd":
                final Set<String> retailerIds = getRetailerIdsToRead();
                if (retailerIds.isEmpty()) {
                    return false;
                }
                if (retailerIds.contains("*")) {
                    return true;
                }
                final var eosName = findAuthorizationProperty(
                        properties, AuthorizationPropertyType.EOS_NAME);
                if (eosName.isPresent() && eosName.get().equals(AuthorizationAttributeType.EOS_RETAILER_ID)) {
                    return true;
                }
                return false;
            default:
                // Other values are not supported or not implemented
                return false;
        }
    }

    private static Optional<String> findAuthorizationProperty(
            final List<AuthorizationProperty> properties,
            final String name) {
        return properties.stream()
                .filter(property -> property.getName().equals(name))
                .map(AuthorizationProperty::getValue)
                .findFirst();
    }

    /**
     * Returns set of allowed business partner ids, only in case if the principal is external ({@code #isExternal}).
     *
     * @return Non-null set external ids
     * @throws PluginException in case if Principal is not external and is not supporting business partner ids
     */
    public abstract Set<String> getBpids() throws PluginException;

    Set<String> getRetailerIdsToRead() throws PluginException {
        if (null == cachedRetailerIds) {
            cachedRetailerIds = retailerIdsSupplier.get();
        }
        return cachedRetailerIds;
    }

    protected boolean isPerEventOperationAllowed(
            final AuthorizationService.Operation operation,
            final AuthorizationAttribute attribute)
            throws PluginException {

        if (!Objects.equals(attribute.getDataType(), AuthorizationAttributeType.EOS_RETAILER_ID)) {
            return false;
        }
        switch (operation) {
            case READ:
                final Set<String> retailerIds = getRetailerIdsToRead();
                return retailerIds.contains("*") || retailerIds.contains(attribute.getValue());
            case WRITE:
                return true;
            default:
                return false;
        }
    }
}
