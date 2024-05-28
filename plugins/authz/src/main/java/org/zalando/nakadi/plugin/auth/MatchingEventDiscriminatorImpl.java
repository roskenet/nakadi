package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.authz.MatchingEventDiscriminator;

import java.util.Set;

public class MatchingEventDiscriminatorImpl implements MatchingEventDiscriminator {
    private final String name;
    private final Set<String> values;

    public MatchingEventDiscriminatorImpl(final String name,
                                          final Set<String> values) {
        this.name = name;
        this.values = values;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<String> getValues() {
        return values;
    }
}
