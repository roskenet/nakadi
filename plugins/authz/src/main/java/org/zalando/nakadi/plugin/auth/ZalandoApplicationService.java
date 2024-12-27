package org.zalando.nakadi.plugin.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.util.Optional;

public class ZalandoApplicationService implements ApplicationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZalandoApplicationService.class);

    private final KioService kioService;
    private final ZalandoTeamService zalandoTeamService;

    public ZalandoApplicationService(
            final KioService kioService,
            final ZalandoTeamService zalandoTeamService) {
        this.kioService = kioService;
        this.zalandoTeamService = zalandoTeamService;
    }

    @Override
    public boolean exists(final String applicationId) throws PluginException {
        if (applicationId == null) {
            LOGGER.warn("null application id is not valid");
            return false;
        }

        final String actualValueToCheck = removeStupsPrefix(applicationId);
        if (kioService.exists(actualValueToCheck)) {
            return true;
        }
        LOGGER.warn("Application is not found: {} (checked like '{}')", applicationId, actualValueToCheck);
        return false;
    }

    @Override
    public Optional<String> getOwningTeamId(final String applicationId) throws PluginException {
        if (applicationId == null) {
            LOGGER.warn("null application id is not valid");
            return Optional.empty();
        }

        final String actualValueToCheck = removeStupsPrefix(applicationId);
        final Optional<String> teamId = kioService.getOwningTeam(actualValueToCheck);
        if (teamId.isPresent()) {
            return teamId.flatMap(zalandoTeamService::getOfficialTeamId);
        }
        LOGGER.warn("Owning team of the application is not found: {} (checked like '{}')",
                applicationId, actualValueToCheck);
        return Optional.empty();
    }

    private static String removeStupsPrefix(final String applicationId) {
        return applicationId.startsWith(TokenAuthorizationService.SERVICE_PREFIX)
                ? applicationId.substring(TokenAuthorizationService.SERVICE_PREFIX.length())
                : applicationId;
    }
}
