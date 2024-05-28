package org.zalando.nakadi.plugin.api.authz;

import java.util.Set;

public interface MatchingEventDiscriminator {

    String getName();

    Set<String> getValues();
}
