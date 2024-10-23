package org.zalando.nakadi.plugin.auth;

import org.jetbrains.annotations.NotNull;
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
        LOGGER.info("Application is not found: {}({})", applicationId, actualValueToCheck);
        return false;
    }

    @Override
    public Optional<String> getOwningTeamId(final String applicationId) {
        final Optional<String> optionalOwningTeam =
                kioService.getOwningTeam(removeStupsPrefix(applicationId));
        return optionalOwningTeam.flatMap(zalandoTeamService::getOfficialTeamId);
    }

    @NotNull
    private static String removeStupsPrefix(final String applicationId) {
        final String actualValueToCheck;
        if (applicationId.startsWith(TokenAuthorizationService.SERVICE_PREFIX)) {
            actualValueToCheck = applicationId.substring(TokenAuthorizationService.SERVICE_PREFIX.length());
        } else {
            actualValueToCheck = applicationId;
        }
        return actualValueToCheck;
    }
}
