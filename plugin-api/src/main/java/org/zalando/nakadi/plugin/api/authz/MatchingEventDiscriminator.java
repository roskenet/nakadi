package org.zalando.nakadi.plugin.api.authz;

import java.util.Objects;
import java.util.Set;

public class MatchingEventDiscriminator {
    private final String name;
    private final Set<String> values;

    public MatchingEventDiscriminator(final String name,
                                      final Set<String> values) {
        this.name = name;
        this.values = values;
    }

    public String getName() {
        return name;
    }

    public Set<String> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "MatchingEventDiscriminator {" +
                "name='" + name + '\'' +
                ", values=" + values +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MatchingEventDiscriminator)) {
            return false;
        }
        final MatchingEventDiscriminator that = (MatchingEventDiscriminator) o;
        return Objects.equals(getName(), that.getName()) &&
                Objects.equals(getValues(), that.getValues());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getValues());
    }
}
