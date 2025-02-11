package org.zalando.nakadi.plugin.auth;

import okhttp3.OkHttpClient;
import org.zalando.nakadi.plugin.api.SystemProperties;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationServiceFactory;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

public class TokenAuthorizationServiceFactory implements AuthorizationServiceFactory {

    @Override
    public AuthorizationService init(final SystemProperties properties) {
        final ServiceFactory serviceFactory = new ServiceFactory(properties);
        final boolean denyUserAdminOperations = serviceFactory.getOrDefaultProperty(
                "nakadi.plugins.authz.deny-user-admin-operations", Boolean::valueOf, true);
        final String usersType = serviceFactory.getProperty("nakadi.plugins.authz.users-type");
        final String servicesType = serviceFactory.getProperty("nakadi.plugins.authz.services-type");
        final String businessPartnersType = serviceFactory.getProperty("nakadi.plugins.authz.business-partners-type");

        final List<String> merchantUids = Arrays.asList(
                serviceFactory.getProperty("nakadi.plugins.authz.merchant.uids").split("\\s*,\\s*"));

        try {
            return new TokenAuthorizationService(
                    denyUserAdminOperations,
                    usersType, serviceFactory.getOrCreateUserRegistry(),
                    servicesType, serviceFactory.getOrCreateKioService(),
                    businessPartnersType, serviceFactory.getOrCreateMerchantRegistry(),
                    serviceFactory.getOrCreateZalandoTeamService(),
                    createOpaClient(serviceFactory),
                    merchantUids);
        } catch (URISyntaxException e) {
            throw new PluginException(e);
        }
    }

    private OPAClient createOpaClient(final ServiceFactory factory) {
        final BiFunction<String, Long, Long> getLong =
                (prop, defaultValue) -> factory.getOrDefaultProperty(prop, Long::valueOf, defaultValue);
        try {
            final String endpoint = factory.getProperty("nakadi.plugins.authz.opa.endpoint");
            final String policyPath = factory.getProperty("nakadi.plugins.authz.opa.policypath");
            final TokenProvider tokenProvider = factory.getOrCreateTokenProvider();
            final long timeoutConnect = getLong.apply("nakadi.plugins.authz.opa.timeoutms.connect", 60L);
            final long timeoutWrite = getLong.apply("nakadi.plugins.authz.opa.timeoutms.write", 60L);
            final long timeoutRead = getLong.apply("nakadi.plugins.authz.opa.timeoutms.read", 80L);
            final long retryTimeout = getLong.apply("nakadi.plugins.authz.opa.retry.timeout", 100L);
            final int retryTimes = factory.getOrDefaultProperty("nakadi.plugins.authz.opa.retry.times",
                    Integer::valueOf, 1);
            final String opaDegradationPolicy = factory.getOrDefaultProperty("nakadi.plugins.authz.opa.degradation",
                    Function.identity(), "THROW");

            final OpaDegradationPolicy policy =  OpaDegradationPolicy.from(opaDegradationPolicy).get();
            final OkHttpClient client = new OkHttpClient.Builder()
                    .connectTimeout(timeoutConnect, TimeUnit.MILLISECONDS)
                    .writeTimeout(timeoutWrite, TimeUnit.MILLISECONDS)
                    .readTimeout(timeoutRead, TimeUnit.MILLISECONDS)
                    .retryOnConnectionFailure(false)
                    .build();
            return new OPAClient(client, tokenProvider, endpoint, policyPath, retryTimeout, retryTimes, policy);
        } catch (URISyntaxException e) {
            throw new PluginException(e);
        }
    }

}
