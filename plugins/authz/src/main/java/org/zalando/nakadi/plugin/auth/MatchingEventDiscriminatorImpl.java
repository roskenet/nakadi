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
    public String toString() {
        return "MatchingEventDiscriminatorImpl{" +
                "name='" + name + '\'' +
                ", values=" + values +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MatchingEventDiscriminatorImpl)) {
            return false;
        }

        final MatchingEventDiscriminatorImpl that = (MatchingEventDiscriminatorImpl) o;

        if (!getName().equals(that.getName())) {
            return false;
        }
        return getValues().equals(that.getValues());
    }

    @Override
    public int hashCode() {
        int result = getName().hashCode();
        result = 31 * result + getValues().hashCode();
        return result;
    }
}
