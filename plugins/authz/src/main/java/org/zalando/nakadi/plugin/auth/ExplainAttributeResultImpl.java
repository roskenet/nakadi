package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.authz.ExplainAttributeResult;
import org.zalando.nakadi.plugin.api.authz.MatchingEventDiscriminator;

import java.util.List;
import java.util.Objects;

public class ExplainAttributeResultImpl implements ExplainAttributeResult {
   private AccessLevel accessLevel;
   private AccessRestrictionType accessRestrictionType;
   private String reason;
   private List<MatchingEventDiscriminator> matchingEventDiscriminators;

    public ExplainAttributeResultImpl(final AccessLevel accessLevel,
                                      final AccessRestrictionType accessRestrictionType,
                                      final String reason,
                                      final List<MatchingEventDiscriminator> matchingEventDiscriminators) {
        this.accessLevel = accessLevel;
        this.accessRestrictionType = accessRestrictionType;
        this.reason = reason;
        this.matchingEventDiscriminators = matchingEventDiscriminators;
    }

    @Override
    public AccessLevel getAccessLevel() {
        return accessLevel;
    }

    @Override
    public AccessRestrictionType getAccessRestrictionType() {
        return accessRestrictionType;
    }

    @Override
    public List<MatchingEventDiscriminator> getMatchingEventDiscriminators() {
        return matchingEventDiscriminators;
    }

    @Override
    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "ExplainAttributeResultImpl{" +
                "accessLevel=" + accessLevel +
                ", accessRestrictionType=" + accessRestrictionType +
                ", reason='" + reason + '\'' +
                ", matchingEventDiscriminators=" + matchingEventDiscriminators +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExplainAttributeResultImpl)) {
            return false;
        }
        final ExplainAttributeResultImpl that = (ExplainAttributeResultImpl) o;
        return getAccessLevel() == that.getAccessLevel() &&
                getAccessRestrictionType() == that.getAccessRestrictionType() &&
                Objects.equals(getMatchingEventDiscriminators(),
                        that.getMatchingEventDiscriminators());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getAccessLevel(),
                getAccessRestrictionType(),
                getMatchingEventDiscriminators());
    }
}
