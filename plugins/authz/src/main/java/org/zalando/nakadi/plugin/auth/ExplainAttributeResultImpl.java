package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.authz.ExplainAttributeResult;
import org.zalando.nakadi.plugin.api.authz.MatchingEventDiscriminator;

import java.util.List;

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExplainAttributeResultImpl that = (ExplainAttributeResultImpl) o;

        if (accessLevel != that.accessLevel) return false;
        if (accessRestrictionType != that.accessRestrictionType) return false;
        if (!reason.equals(that.reason)) return false;
        return matchingEventDiscriminators.equals(that.matchingEventDiscriminators);
    }

    @Override
    public int hashCode() {
        int result = accessLevel.hashCode();
        result = 31 * result + accessRestrictionType.hashCode();
        result = 31 * result + reason.hashCode();
        result = 31 * result + matchingEventDiscriminators.hashCode();
        return result;
    }
}
