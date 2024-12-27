package org.zalando.nakadi.plugin.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class KioService {
    private static final Logger LOGGER = LoggerFactory.getLogger(KioService.class);
    private final String kioUrl;
    private final HttpClient httpClient;
    private final TokenProvider tokenProvider;
    private final ObjectMapper objectMapper;
    private final LoadingCache<String, Optional<KioApplication>> valueCache;

    public KioService(
            final String kioEndpoint,
            final HttpClient httpClient,
            final TokenProvider tokenProvider,
            final ObjectMapper objectMapper) {

        this.kioUrl = kioEndpoint.endsWith("/") ? kioEndpoint : kioEndpoint + "/";
        this.httpClient = httpClient;
        this.tokenProvider = tokenProvider;
        this.objectMapper = objectMapper;

        this.valueCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Override
                    public Optional<KioApplication> load(final String key) {
                        return fetchKioApp(key);
                    }
                });
    }

    public boolean exists(final String applicationId) {
        return getCachedApplication(applicationId).isPresent();
    }

    public Optional<String> getOwningTeam(final String applicationId) {
        return getCachedApplication(applicationId).map(app -> app.teamId);
    }

    private Optional<KioApplication> getCachedApplication(final String applicationId) {
        try {
            return valueCache.get(applicationId);
        } catch (final Exception e) {     // it may throw com.google.common.util.concurrent.UncheckedExecutionException
            throw new PluginException(e); // may double-wrap PluginException, but that's fine
        }
    }

    private Optional<KioApplication> fetchKioApp(final String applicationId) {
        final HttpGet request = new HttpGet(kioUrl + applicationId);
        request.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
        request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + tokenProvider.getToken());
        try {
            final HttpResponse response = httpClient.execute(request);
            final String responseBody = EntityUtils.toString(response.getEntity());
            final int statusCode = response.getStatusLine().getStatusCode();
            LOGGER.debug("Got HTTP {} from {} for applicationId {}; body={}",
                    statusCode, kioUrl, applicationId, responseBody);
            if (statusCode == HttpStatus.SC_OK) {
                return Optional.of(objectMapper.readValue(responseBody, KioApplication.class));
            } else if (statusCode == HttpStatus.SC_NOT_FOUND) {
                return Optional.empty();
            } else {
                throw new PluginException("Incorrect status code " + statusCode + " when validating " +
                        applicationId + " against " + kioUrl + ". Response body: " + responseBody);
            }
        } catch (final JsonProcessingException jpe) {
            throw new PluginException("Failed to parse response for " + applicationId + " from " + kioUrl, jpe);
        } catch (final IOException ex) {
            throw new PluginException(
                    "Failed to fetch " + applicationId + " from the endpoint " + kioUrl, ex);
        } finally {
            request.releaseConnection();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class KioApplication {

        private final String teamId;

        @JsonCreator
        public KioApplication(@JsonProperty("team_id") final String teamId) {
            this.teamId = teamId;
        }
    }
}
