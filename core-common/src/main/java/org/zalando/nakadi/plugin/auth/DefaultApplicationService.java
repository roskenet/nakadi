package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.util.Optional;

public class DefaultApplicationService implements ApplicationService {

    @Override
    public boolean exists(final String applicationId) {
        return true;
    }

    @Override
    public Optional<String> getOwningTeamId(final String applicationId) throws PluginException {
        return Optional.empty();
    }
}
