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
}
