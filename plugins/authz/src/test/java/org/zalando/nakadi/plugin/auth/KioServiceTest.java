package org.zalando.nakadi.plugin.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;
import org.zalando.nakadi.plugin.auth.utils.Fixture;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KioServiceTest {
    private static final String MOCK_TOKEN = UUID.randomUUID().toString();

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8091);
    private final TokenProvider tokenProvider = mock(TokenProvider.class);

    private KioService kioService;

    @Before
    public void init() {
        when(tokenProvider.getToken()).thenReturn(MOCK_TOKEN);
        this.kioService = new KioService(
                wireMockRule.baseUrl(),
                HttpClientBuilder.create()
                        .setUserAgent("nakadi")
                        .evictIdleConnections(1L, TimeUnit.MINUTES)
                        .evictExpiredConnections()
                        .build(),
                tokenProvider,
                new ObjectMapper());
    }

    @Test
    public void existsShouldReturnFalseWhenAppDoesNotMatchPattern() {
        assertTrue(KioService.isValidFormat("nakadi"));
        assertTrue(KioService.isValidFormat("nakadi-sql"));
        assertTrue(KioService.isValidFormat("nakadi2"));
        assertFalse(KioService.isValidFormat("XXX"));
        assertFalse(KioService.isValidFormat("some_thing"));
        assertFalse(KioService.isValidFormat("other.thing"));
        assertFalse(KioService.isValidFormat("012345abc"));
        assertFalse(KioService.isValidFormat("nakadi "));
    }

    @Test
    public void existsShouldReturnTrueIfKioAppExists() throws Throwable {
        final String owningApplication = "aruha";
        WireMock.stubFor(get(urlEqualTo("/" + owningApplication))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(Fixture.fixture("/fixtures/kio-app-response.json"))));
        assertTrue(kioService.exists(owningApplication));
    }

    @Test
    public void existsShouldReturnFalseWhenAppDoesNotExist() throws Throwable {
        final String owningApplication = "incorrect-application";
        WireMock.stubFor(get(urlEqualTo("/" + owningApplication))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse()
                        .withStatus(404)
                        .withBody("{}")));
        assertFalse(kioService.exists(owningApplication));
    }

    @Test
    public void existsShouldThrowPluginExceptionWhenKioResponseIsMalformed() {
        final String owningApplication = "incorrect-application";
        WireMock.stubFor(get(urlEqualTo("/" + owningApplication))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("{")));
        final PluginException pluginException = assertThrows(
                PluginException.class,
                () -> kioService.exists(owningApplication)
        );
    }

    @Test
    public void existsShouldThrowPluginExceptionWhenHttpClientThrowsIOException() {
        final String owningApplication = "incorrect-application";
        WireMock.stubFor(get(urlEqualTo("/" + owningApplication))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));
        final PluginException pluginException = assertThrows(
                PluginException.class,
                () -> kioService.exists(owningApplication)
        );
    }

    @Test
    public void getOwningTeamShouldFetchTheTeamId() throws Throwable {
        final String owningApplication = "aruha";
        WireMock.stubFor(get(urlEqualTo("/" + owningApplication))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(Fixture.fixture("/fixtures/kio-app-response.json"))));
        assertEquals(Optional.of("aruha"), kioService.getOwningTeam(owningApplication));
    }

    @Test
    public void getOwningTeamShouldReturnEmptyOptionalWhenAppDoesNotExist() throws Throwable {
        final String owningApplication = "unknown-application";
        WireMock.stubFor(get(urlEqualTo("/" + owningApplication))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse()
                        .withStatus(404)
                        .withBody("{}")));
        assertEquals(Optional.empty(), kioService.getOwningTeam(owningApplication));
    }

    @Test
    public void getOwningTeamShouldThrowPluginExceptionWhenKioResponseIsMalformed() {
        final String owningApplication = "unknown-application";
        WireMock.stubFor(get(urlEqualTo("/" + owningApplication))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("{")));
        final PluginException pluginException = assertThrows(
                PluginException.class,
                () -> kioService.getOwningTeam(owningApplication)
        );
    }

    @Test
    public void getOwningTeamShouldThrowPluginExceptionWhenHttpClientThrowsIOException() {
        final String owningApplication = "unknown-application";
        WireMock.stubFor(get(urlEqualTo("/" + owningApplication))
                .withHeader("Authorization", equalTo("Bearer " + MOCK_TOKEN))
                .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));
        final PluginException pluginException = assertThrows(
                PluginException.class,
                () -> kioService.getOwningTeam(owningApplication)
        );
    }
}
