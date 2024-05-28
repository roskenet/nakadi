package org.zalando.nakadi.plugin.api.authz;

import java.util.List;

public interface ExplainAttributeResult {

    enum AccessLevel {
        FULL_ACCESS, RESTRICTED_ACCESS, NO_ACCESS
    }

    enum AccessRestrictionType {
        MATCHING_EVENT_DISCRIMINATORS
    }

    AccessLevel getAccessLevel();

    AccessRestrictionType getAccessRestrictionType();

    List<MatchingEventDiscriminator> getMatchingEventDiscriminators();

    String getReason();
}
