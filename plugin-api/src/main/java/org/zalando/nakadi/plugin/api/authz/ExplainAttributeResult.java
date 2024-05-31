package org.zalando.nakadi.plugin.api.authz;

import java.util.List;
import java.util.Objects;

public class ExplainAttributeResult {

    public enum AccessLevel {
        FULL_ACCESS, RESTRICTED_ACCESS, NO_ACCESS
    }

    public enum AccessRestrictionType {
        MATCHING_EVENT_DISCRIMINATORS
    }

    private AccessLevel accessLevel;
    private AccessRestrictionType accessRestrictionType;
    private String reason;
    private List<MatchingEventDiscriminator> matchingEventDiscriminators;

    public ExplainAttributeResult(final AccessLevel accessLevel,
                                  final AccessRestrictionType accessRestrictionType,
                                  final String reason,
                                  final List<MatchingEventDiscriminator> matchingEventDiscriminators) {
        this.accessLevel = accessLevel;
        this.accessRestrictionType = accessRestrictionType;
        this.reason = reason;
        this.matchingEventDiscriminators = matchingEventDiscriminators;
    }

    public AccessLevel getAccessLevel() {
        return accessLevel;
    }

    public AccessRestrictionType getAccessRestrictionType() {
        return accessRestrictionType;
    }

    public List<MatchingEventDiscriminator> getMatchingEventDiscriminators() {
        return matchingEventDiscriminators;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "ExplainAttributeResult {" +
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
        if (!(o instanceof ExplainAttributeResult)) {
            return false;
        }
        final ExplainAttributeResult that = (ExplainAttributeResult) o;
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
