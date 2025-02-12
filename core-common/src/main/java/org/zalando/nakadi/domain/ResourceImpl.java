package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationProperty;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ResourceImpl<T> implements Resource<T> {

    private final T resource;
    private final String name;
    private final String type;
    private final ValidatableAuthorization authorization;
    private final List<AuthorizationProperty> properties;

    public ResourceImpl(final String name, final String type,
                        @Nullable final ValidatableAuthorization authorization, final T resource) {
        this(name, type, authorization, resource, Collections.emptyList());
    }

    public ResourceImpl(final String name, final String type,
                        @Nullable final ValidatableAuthorization authorization, final T resource,
                        final List<AuthorizationProperty> properties) {
        this.name = name;
        this.type = type;
        this.authorization = authorization;
        this.resource = resource;
        this.properties = properties;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        if (null == authorization) {
            return Optional.empty();
        }
        return authorization.getAttributesForOperation(operation);
    }

    @Override
    public Map<String, List<AuthorizationAttribute>> getAuthorization() {
        if (authorization != null) {
            return authorization.asMapValue();
        }
        return null;
    }

    @Override
    public List<AuthorizationProperty> getProperties() {
        return properties;
    }

    @Override
    public T get() {
        return resource;
    }

    @Override
    public String toString() {
        return "AuthorizedResource{" + type + "='" + name + "'}";
    }
}
