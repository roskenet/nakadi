package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.ApplicationServiceFactory;
import org.zalando.nakadi.plugin.api.SystemProperties;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

public class ZalandoApplicationServiceFactory implements ApplicationServiceFactory {
    @Override
    public ApplicationService init(final SystemProperties properties) throws PluginException {
        final ServiceFactory serviceFactory = new ServiceFactory(properties);
        return new ZalandoApplicationService(
                serviceFactory.getOrCreateKioService(),
                serviceFactory.getOrCreateZalandoTeamService()
        );
    }
}
