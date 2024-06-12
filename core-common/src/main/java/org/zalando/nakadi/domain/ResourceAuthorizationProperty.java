package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationProperty;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

@Immutable
public class ResourceAuthorizationProperty implements AuthorizationProperty {
    @NotNull
    private final String name;
    @NotNull
    private final String value;

    public ResourceAuthorizationProperty(final String name, final String value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getValue() {
        return value;
    }
}
