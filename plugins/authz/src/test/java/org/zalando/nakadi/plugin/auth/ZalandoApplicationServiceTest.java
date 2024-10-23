package org.zalando.nakadi.plugin.auth;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ZalandoApplicationServiceTest {
    @Mock
    private KioService kioService;
    @Mock
    private ZalandoTeamService zalandoTeamService;

    private ZalandoApplicationService zalandoApplicationService;

    @Before
    public void setupBean() {
        zalandoApplicationService = new ZalandoApplicationService(
                kioService,
                zalandoTeamService
        );
    }

    @Test
    public void verifyNullIsFiltered() {
        assertFalse(zalandoApplicationService.exists(null));
        verifyNoInteractions(kioService);
    }

    @Test
    public void verifySuccessStupsIsReplaced() {
        when(kioService.exists("nakadi")).thenReturn(true);

        assertTrue(zalandoApplicationService.exists("stups_nakadi"));
    }

    @Test
    public void verifySuccessStupsIsNotReplaced() {
        when(kioService.exists("stupsnakadi")).thenReturn(true);

        assertTrue(zalandoApplicationService.exists("stupsnakadi"));
    }

    @Test
    public void verifyFalseIsPropagated() {
        when(kioService.exists("not-found")).thenReturn(false);
        assertFalse(zalandoApplicationService.exists("not-found"));
    }

    @Test
    public void verifyExceptionIsPropagated() {
        when(kioService.exists(eq("exceptional"))).thenThrow(new PluginException("TestException"));
        final PluginException e = assertThrows(PluginException.class,
                () -> zalandoApplicationService.exists("exceptional"));
        assertEquals("TestException", e.getMessage());
    }

    @Test
    public void verifyGetOfficialTeamIdReturnsTeamId() {
        final String owningApplication = "nakadi";
        final String teamName = "aruha";
        final String expectedTeamId = "98765";
        when(kioService.getOwningTeam(owningApplication)).thenReturn(Optional.of(teamName));
        when(zalandoTeamService.getOfficialTeamId(teamName)).thenReturn(Optional.of(expectedTeamId));

        final Optional<String> actualTeamId = zalandoApplicationService.getOwningTeamId(owningApplication);
        assertEquals(Optional.of(expectedTeamId), actualTeamId);
    }

    @Test
    public void verifyStupsIsReplacedInGetOfficialTeam() {
        final String owningApplication = "stups_nakadi";
        final String applicationWithoutStups = "nakadi";
        final String teamName = "aruha";
        final String expectedTeamId = "98765";
        when(kioService.getOwningTeam(applicationWithoutStups)).thenReturn(Optional.of(teamName));
        when(zalandoTeamService.getOfficialTeamId(teamName)).thenReturn(Optional.of(expectedTeamId));

        final Optional<String> actualTeamId = zalandoApplicationService.getOwningTeamId(owningApplication);
        assertEquals(Optional.of(expectedTeamId), actualTeamId);
    }
}