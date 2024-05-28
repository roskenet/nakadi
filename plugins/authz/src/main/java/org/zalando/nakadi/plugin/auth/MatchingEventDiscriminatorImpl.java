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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchingEventDiscriminatorImpl that = (MatchingEventDiscriminatorImpl) o;

        if (!name.equals(that.name)) return false;
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + values.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MatchingEventDiscriminatorImpl{" +
                "name='" + name + '\'' +
                ", values=" + values +
                '}';
    }
}
