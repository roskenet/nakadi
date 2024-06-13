package org.zalando.nakadi.plugin.auth;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationProperty;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.ExplainAttributeResult;
import org.zalando.nakadi.plugin.api.authz.ExplainResourceResult;
import org.zalando.nakadi.plugin.api.authz.MatchingEventDiscriminator;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.exceptions.AuthorizationInvalidException;
import org.zalando.nakadi.plugin.api.exceptions.OperationOnResourceNotPermittedException;
import org.zalando.nakadi.plugin.auth.attribute.SimpleAuthorizationAttribute;
import org.zalando.nakadi.plugin.auth.subject.EmployeeSubject;
import org.zalando.nakadi.plugin.auth.subject.Principal;
import org.zalando.nakadi.plugin.auth.utils.ResourceBuilder;
import org.zalando.nakadi.plugin.auth.utils.SimpleEventResource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.plugin.api.authz.AccessLevel.FULL_ACCESS;
import static org.zalando.nakadi.plugin.api.authz.AccessLevel.NO_ACCESS;
import static org.zalando.nakadi.plugin.api.authz.AccessLevel.RESTRICTED_ACCESS;
import static org.zalando.nakadi.plugin.api.authz.ExplainAttributeResult.AccessRestrictionType.MATCHING_EVENT_DISCRIMINATORS;
import static org.zalando.nakadi.plugin.auth.ResourceType.ALL_DATA_ACCESS_RESOURCE;
import static org.zalando.nakadi.plugin.auth.utils.ResourceBuilder.rb;

@RunWith(MockitoJUnitRunner.class)
public class TokenAuthorizationServiceTest {

    private static final String BUSINESS_PARTNER_TYPE = "business_partner";
    public static final String SERVICES_TYPE = "services";
    public static final String USERS_TYPE = "users";

    private TokenAuthorizationService authzService;
    @Mock
    private Authentication authentication;
    @Mock
    private TokenProvider tokenProvider;
    @Mock
    private ValueRegistry userRegistry;
    @Mock
    private ValueRegistry serviceRegistry;
    @Mock
    private ValueRegistry merchantRegistry;
    @Mock
    private Principal principal;
    @Mock
    private ZalandoTeamService teamService;

    @Mock
    private OPAClient opaClient;

    @Before
    public void setUp() {
        when(authentication.getPrincipal()).thenReturn(principal);

        authzService = new TokenAuthorizationService(
                USERS_TYPE, userRegistry,
                SERVICES_TYPE, serviceRegistry,
                BUSINESS_PARTNER_TYPE, merchantRegistry,
                teamService,
                opaClient,
                Arrays.asList("stups_merchant-uid"));

        SecurityContextHolder.getContext().setAuthentication(new OAuth2Authentication(null, authentication));
    }

    @Test
    public void serviceWithoutStupsPrefixDoesNotExist() {
        final Resource<?> r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, SERVICES_TYPE, "nakadi")
                .build();

        when(principal.isExternal()).thenReturn(false);
        when(serviceRegistry.isValid(anyString())).thenReturn(true);

        // serviceRegistry should be called, so that mockito is not reporting about isValid not called.
        authzService.isAuthorizationForResourceValid(rb("warmup-mockito", "event-type")
                .add(AuthorizationService.Operation.READ, SERVICES_TYPE, "stups_nakadi")
                .build());

        final AuthorizationInvalidException e = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));

        assertThat(e.getMessage(),
                equalTo("authorization attribute services:nakadi is invalid"));
    }

    @Test
    public void testServiceValidationExecuted() {
        final Resource<?> r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, SERVICES_TYPE, "stups_idonotexist")
                .build();
        when(principal.isExternal()).thenReturn(false);

        when(serviceRegistry.isValid(eq("idonotexist"))).thenReturn(false);
        assertThrows(AuthorizationInvalidException.class, () -> authzService.isAuthorizationForResourceValid(r));

        when(serviceRegistry.isValid(eq("idonotexist"))).thenReturn(true);
        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void serviceWithUUIDIsValid() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, SERVICES_TYPE, "eda0f39a-8b22-48d8-b9f9-aed62f15e2cf")
                .build();

        when(principal.isExternal()).thenReturn(false);

        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void gatewayServiceIsNotValid() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, SERVICES_TYPE, "stups_merchant-uid")
                .build();
        when(serviceRegistry.isValid(eq("merchant-uid"))).thenReturn(true);

        final AuthorizationInvalidException e = assertThrows(
                AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(),
                equalTo("Gateway is not allowed in authorization section"));
    }

    @Test
    public void whenAuthCallSucceedsThenSuccess() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, USERS_TYPE, "user1")
                .build();
        when(
                principal.isAuthorized(
                        eq("event-type"),
                        eq(AuthorizationService.Operation.READ),
                        eq(Optional.of(Arrays.asList(new SimpleAuthorizationAttribute(USERS_TYPE, "user1")))),
                        anyListOf(AuthorizationProperty.class)
                        ))
                .thenReturn(true);

        assertTrue(authzService.isAuthorized(AuthorizationService.Operation.READ, r));
    }

    @Test
    public void whenPrincipalOauthCallFailsThenFail() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, USERS_TYPE, "user1")
                .build();
        when(
                principal.isAuthorized(
                        eq("event-type"),
                        eq(AuthorizationService.Operation.READ),
                        eq(Optional.of(Arrays.asList(new SimpleAuthorizationAttribute(USERS_TYPE, "user1")))),
                        anyListOf(AuthorizationProperty.class)
                ))
                .thenReturn(false);

        assertFalse(authzService.isAuthorized(AuthorizationService.Operation.READ, r));
    }

    @Test
    public void testBusinessPartnerIsValid() {
        final Resource r = rb("myResource1", "event-type")
                .resource(new SimpleEventResource("COMPATIBLE", "DELETE"))
                .add(AuthorizationService.Operation.READ, BUSINESS_PARTNER_TYPE, "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .build();
        when(merchantRegistry.isValid(eq("ahzhd657-dhsdjs-dshd83-dhsdjs"))).thenReturn(true);
        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void testBPDoesNotOccurWithWildCardForEventType() throws Exception {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.ADMIN, "business_partner", "abcde")
                .add(AuthorizationService.Operation.READ, "*", "*")
                .resource(new SimpleEventResource("COMPATIBLE", "DELETE"))
                .build();
        when(merchantRegistry.isValid(eq("abcde"))).thenReturn(true);

        final AuthorizationInvalidException e = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(),
                equalTo("Business Partner cannot be present with wild card in authorization"));
    }

    @Test
    public void testAuthorizationHasAtMostOneBPForOneOperationForEventType() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, "business_partner", "abcde")
                .add(AuthorizationService.Operation.READ, "business_partner", "abc")
                .resource(new SimpleEventResource("COMPATIBLE", "DELETE"))
                .build();
        when(merchantRegistry.isValid(any())).thenReturn(true);

        final AuthorizationInvalidException e = assertThrows(
                AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(), equalTo("Only one business partner allowed for each operation"));
    }

    @Test
    public void testSuccessfulCreationForSubscription() {
        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.READ, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .add(AuthorizationService.Operation.ADMIN, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .build();

        when(principal.isExternal()).thenReturn(true);
        when(principal.getBpids()).thenReturn(Collections.singleton("ahzhd657-dhsdjs-dshd83-dhsdjs"));
        when(merchantRegistry.isValid(eq("ahzhd657-dhsdjs-dshd83-dhsdjs"))).thenReturn(true);

        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void testSameBPInAdminAndReaderForSubscription() {
        when(principal.isExternal()).thenReturn(true);
        when(principal.getBpids()).thenReturn(Collections.singleton("ahzhd657-dhsdjs-dshd83-dhsdjs"));
        when(merchantRegistry.isValid(eq("ahzhd657-dhsdjs-dshd83-dhsdjs"))).thenReturn(true);
        when(merchantRegistry.isValid(eq("abc"))).thenReturn(true);

        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.READ, "business_partner", "abc")
                .add(AuthorizationService.Operation.ADMIN, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .build();
        final AuthorizationInvalidException e1 = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e1.getMessage(),
                equalTo("Business partner must only add itself as both admin and reader"));
    }

    @Test
    public void testSameOneBPInAdminAndReaderForSubscription() {
        when(principal.isExternal()).thenReturn(true);
        when(principal.getBpids()).thenReturn(Collections.singleton("ahzhd657-dhsdjs-dshd83-dhsdjs"));
        when(merchantRegistry.isValid(eq("ahzhd657-dhsdjs-dshd83-dhsdjs"))).thenReturn(true);

        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.READ, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .add(AuthorizationService.Operation.READ, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .add(AuthorizationService.Operation.ADMIN, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .build();

        final AuthorizationInvalidException e2 = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e2.getMessage(),
                equalTo("Business partner must only add itself as both admin and reader"));
    }

    @Test
    public void testEmptyAuthorizationForBP() {
        final Resource r = rb("myResource1", "event-type").build();
        when(principal.isExternal()).thenReturn(true);

        final AuthorizationInvalidException e = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(), equalTo("Empty authorization is not allowed"));
    }

    @Test
    public void testZalandoEmployeeCannotCreateSubscription() {
        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.READ, BUSINESS_PARTNER_TYPE, "abcde")
                .build();

        when(merchantRegistry.isValid(any())).thenReturn(true);
        when(principal.isExternal()).thenReturn(false);

        final OperationOnResourceNotPermittedException e = assertThrows(OperationOnResourceNotPermittedException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(), equalTo("Subscription including business " +
                "partner can be only created by corresponding business partner"));
    }

    @Test
    public void testBPCanCreateSubscriptionWithValidCredentials() {
        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.READ, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .add(AuthorizationService.Operation.ADMIN, "business_partner", "ahzhd657-dhsdjs-dshd83-dhsdjs")
                .build();
        when(principal.getBpids()).thenReturn(Collections.singleton("ahzhd657-dhsdjs-dshd83-dhsdjs"));
        when(principal.isExternal()).thenReturn(true);
        when(merchantRegistry.isValid(any())).thenReturn(true);

        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void testBPCanNotCreateSubscriptionWithInvalidCredentials() {
        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.ADMIN, "services", "stups_nakadi")
                .add(AuthorizationService.Operation.READ, "*", "*")
                .build();
        when(principal.isExternal()).thenReturn(true);
        when(serviceRegistry.isValid(any())).thenReturn(true);

        final AuthorizationInvalidException e = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(),
                equalTo("Authorization should contain business partner"));
    }

    @Test
    public void testZalandoEmployeeAuthorisationHasMinimumRestriction() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, "services", "stups_nakadi")
                .build();

        when(serviceRegistry.isValid(eq("nakadi"))).thenReturn(true);
        when(principal.isExternal()).thenReturn(false);

        authzService.isAuthorizationForResourceValid(r);

        //For Null Authorization
        authzService.isAuthorizationForResourceValid(rb("myResource1", "event-type").build());
    }

    @Test
    public void testBPCannotAccessAllDataAndPermissionResource() {
        final Resource r = rb("myResource1", ALL_DATA_ACCESS_RESOURCE)
                .add(AuthorizationService.Operation.READ, BUSINESS_PARTNER_TYPE, "abcde")
                .add(AuthorizationService.Operation.WRITE, BUSINESS_PARTNER_TYPE, "abcde")
                .build();
        when(merchantRegistry.isValid("abcde")).thenReturn(true);

        final OperationOnResourceNotPermittedException e = assertThrows(OperationOnResourceNotPermittedException.class,
                () -> authzService.isAuthorizationForResourceValid(r));

        assertThat(e.getMessage(),
                equalTo("Business Partner is not allowed access to the resource"));
    }

    @Test
    public void testBusinessPartnerIsNotFound() {
        final Resource r = rb("myResource1", "subscription")
                .add(AuthorizationService.Operation.ADMIN, BUSINESS_PARTNER_TYPE, "abcde")
                .build();
        when(merchantRegistry.isValid("abcde")).thenReturn(false);

        final AuthorizationInvalidException e = assertThrows(AuthorizationInvalidException.class,
                () -> authzService.isAuthorizationForResourceValid(r));
        assertThat(e.getMessage(),
                equalTo("authorization attribute business_partner:abcde is invalid"));
    }

    @Test
    public void testTeamMemberIsAuthorized() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.WRITE, "team", "aruha")
                .build();
        when(teamService.getTeamMembers("aruha"))
                .thenReturn(Collections.singletonList("auser"));

        when(authentication.getPrincipal())
                .thenReturn(new EmployeeSubject("jdoe", Collections::emptySet, "users", teamService));
        assertFalse("jdoe should not be authorized",
                authzService.isAuthorized(AuthorizationService.Operation.WRITE, r));

        when(authentication.getPrincipal())
                .thenReturn(new EmployeeSubject("auser", Collections::emptySet, "users", teamService));
        assertTrue("auser should be authorized",
                authzService.isAuthorized(AuthorizationService.Operation.WRITE, r));
    }

    @Test
    public void testTeamIsAllowedInAuthorization() {
        final AuthorizationAttribute attribute = new SimpleAuthorizationAttribute("team", "aruha");
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, attribute)
                .build();

        when(teamService.isValidTeam(eq(attribute.getValue()))).thenReturn(true);
        when(principal.isExternal()).thenReturn(false);
        authzService.isAuthorizationForResourceValid(r);
    }

    @Test
    public void testWhenOnlyEmptyTeamInAuthorizationThenNotAuthorized() {
        final Resource r = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.WRITE, "team", "empty-team")
                .build();
        when(teamService.getTeamMembers("empty-team"))
                .thenReturn(Collections.emptyList());

        when(authentication.getPrincipal())
                .thenReturn(new EmployeeSubject("jdoe", Collections::emptySet, "users", teamService));
        assertFalse("jdoe should not be authorized",
                authzService.isAuthorized(AuthorizationService.Operation.WRITE, r));
    }


    @Test
    public void testExplainAuthorizationOnlySupportsEventTypeResource() {
        final Resource r = rb("myResource1", "subscription")
                .build();
        final var exception = assertThrows(IllegalArgumentException.class,
                () -> authzService.explainAuthorization(r));
        assertThat(exception.getMessage(), equalTo("Only resource of type event-type is supported!"));
    }

    @Test
    public void testExplainAuthorizationMCFClassification() {
        final var fooAppRestricted = explainResource(
                authAttr("service", "foo_app_with_r_ids"),
                withRestrictedAccess("retailer_1", "retailer_2"));

        final var barFullAccess = explainResource(
                authAttr("user", "bar_user_star_r_id"),
                withFullAccess("*"));

        final var memberNoAccess = explainResource(
                authAttr("user", "aruha_member_no_r_ids"),
                withNoAccess());

        testExplainAuthorization("mcf-aspd", true,
                fooAppRestricted,
                barFullAccess,
                memberNoAccess);
    }

    @Test
    public void testExplainAuthorizationMCFClassificationWithEOS() {
        final var fooAppNoAccess = explainResource(
                authAttr("service", "foo_app_with_r_ids"),
                withNoAccess("retailer_1", "retailer_2"));

        final var barFullAccess = explainResource(
                authAttr("user", "bar_user_star_r_id"),
                withFullAccess("*"));

        final var memberNoAccess = explainResource(
                authAttr("user", "aruha_member_no_r_ids"),
                withNoAccess());

        testExplainAuthorization("mcf-aspd", false,
                fooAppNoAccess,
                barFullAccess,
                memberNoAccess);
    }

    @Test
    public void testExplainAuthorizationASPDClassification() {
        final var fooAppFullAccess = explainResource(
                authAttr("service", "foo_app_with_r_ids"),
                withFullAccess("retailer_1", "retailer_2"));

        final var barFullAccess = explainResource(
                authAttr("user", "bar_user_star_r_id"),
                withFullAccess("*"));

        final var memberNoAccess = explainResource(
                authAttr("user", "aruha_member_no_r_ids"),
                withNoAccess());
        testExplainAuthorization("aspd", false,
                fooAppFullAccess,
                barFullAccess, memberNoAccess);
    }

    @Test
    public void testExplainAuthorizationNoneClassification() {
        final var fooAppFullAccess = explainResource(
                authAttr("service", "foo_app_with_r_ids"),
                withFullAccess("retailer_1", "retailer_2"));

        final var barFullAccess = explainResource(
                authAttr("user", "bar_user_star_r_id"),
                withFullAccess("*"));
        testExplainAuthorization("none", false, fooAppFullAccess, barFullAccess);
    }

    @Test
    public void testExplainAuthorizationNullClassificationButPathExists() {
        final var fooAppRestrictedAccess = explainResource(
                authAttr("service", "foo_app_with_r_ids"),
                withRestrictedAccess("retailer_1", "retailer_2"));

        final var barFullAccess = explainResource(
                authAttr("user", "bar_user_star_r_id"),
                withFullAccess("*"));
        testExplainAuthorization(null, true, fooAppRestrictedAccess, barFullAccess);
    }

    @Test
    public void testExplainAuthorizationNullClassification() {
        //everyone should have full access
        final var fooAppRestrictedAccess = explainResource(
                authAttr("service", "foo_app_with_r_ids"),
                withFullAccess("retailer_1", "retailer_2"));
        final var barFullAccess = explainResource(
                authAttr("user", "bar_user_star_r_id"),
                withFullAccess("*"));
        final var memberFullAccess = explainResource(
                authAttr("user", "aruha_member_no_r_ids"),
                withFullAccess());
        testExplainAuthorization(null, false,
                fooAppRestrictedAccess,
                barFullAccess, memberFullAccess);
    }

    @Test
    public void testExplainAuthorizationMCFClassificationWithTeam() {
        final var fooAppRestricted =
                explainResource(
                        authAttr("service", "foo_app_with_r_ids"),
                        withRestrictedAccess("retailer_1", "retailer_2"));

        final var memberNoAccess = explainResource(
                authAttr("team", "aruha"),
                authAttr("user", "aruha_member_no_r_ids"),
                withNoAccess());

        final var memberFullAccess = explainResource(
                authAttr("team", "aruha"),
                authAttr("user", "aruha_member_star_r_id"),
                withFullAccess("*"));

        testExplainAuthorization("mcf-aspd", true, fooAppRestricted, memberNoAccess, memberFullAccess);
    }

    // this tests the response when a user is specified as directly in auth section
    // and as well as part of team
    @Test
    public void testExplainAuthorizationMCFClassificationWithTeamMemberDirect() {
        final var fooApp = explainResource(authAttr("service", "foo_app_with_r_ids"),
                withRestrictedAccess("retailer_1", "retailer_2"));

        final var memberNoAccess = explainResource(
                authAttr("team", "aruha"),
                authAttr("user", "aruha_member_no_r_ids"),
                withNoAccess());

        final var memberFullAcess = explainResource(
                authAttr("team", "aruha"),
                authAttr("user", "aruha_member_star_r_id"),
                withFullAccess("*"));

        final var directMemberFullAccess = explainResource(
                authAttr("team", "aruha"),
                authAttr("user", "aruha_member_star_r_id"),
                withFullAccess("*"));

        testExplainAuthorization("mcf-aspd", true,
                fooApp, memberNoAccess, memberFullAcess, directMemberFullAccess);
    }

    public void testExplainAuthorization(final String classification, final boolean eosPathExists,
                                         final ExplainResourceResult... expectedResults) {
        final ResourceBuilder rb = rb("myResource1", "event-type")
                .add(AuthorizationService.Operation.READ, "service", "foo_app_with_r_ids")
                .add(AuthorizationService.Operation.READ, "team", "aruha")
                .add(AuthorizationService.Operation.READ, "user", "bar_user_star_r_id")
                .add(AuthorizationService.Operation.READ, "user", "aruha_member_no_r_ids")
                .add(AuthorizationService.Operation.READ, "user", "aruha_member_star_r_id")
                .add(AuthorizationService.Operation.READ, "user", "direct_aruha_member_star_r_id");

        if (classification != null) {
            rb.addProperty("aspd-classification", classification);
        }
        if (eosPathExists) {
            rb.addProperty("event_owner_selector.name", "some-path");
        }

        when(teamService.getTeamMembers("aruha"))
                .thenReturn(List.of(
                        "aruha_member_no_r_ids",
                        "aruha_member_star_r_id",
                        "direct_aruha_member_star_r_id"));

        final Resource r = rb.build();

        when(opaClient.getRetailerIdsForService(eq("foo_app_with_r_ids"))).
                thenReturn(Set.of("retailer_1", "retailer_2"));
        when(opaClient.getRetailerIdsForUser(eq("bar_user_star_r_id"))).thenReturn(Set.of("*"));
        when(opaClient.getRetailerIdsForUser(eq("aruha_member_no_r_ids"))).thenReturn(new HashSet<>());
        when(opaClient.getRetailerIdsForUser(eq("aruha_member_star_r_id"))).thenReturn(Set.of("*"));

        final var explainList = authzService.explainAuthorization(r);

        final BiFunction<AuthorizationAttribute, Optional<AuthorizationAttribute>, String> getKey =
                (sub, target) -> sub.toString() + (target.isEmpty() ? "" : target.get().toString());
        final var subject = explainList.stream().
                collect(Collectors.
                        groupingBy(res -> getKey.apply(res.getPrimaryAttribute(), res.getTargetAttribute()))).
                entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get(0)));

        for (final var expected : expectedResults) {
            assertThat(subject.get(
                            getKey.apply(
                                    expected.getPrimaryAttribute(),
                                    expected.getTargetAttribute())),
                    equalTo(expected));
        }
    }

    private static ExplainResourceResult explainResource(final AuthorizationAttribute authAttr,
                                                         final AuthorizationAttribute targetAttr,
                                                         final ExplainAttributeResult result) {
        return new ExplainResourceResult(targetAttr, authAttr, result);
    }

    private static ExplainResourceResult explainResource(final AuthorizationAttribute authAttr,
                                                         final ExplainAttributeResult result) {
        return explainResource(authAttr, null, result);
    }

    private static ExplainAttributeResult withRestrictedAccess(final String... retailerIds) {
        return new ExplainAttributeResult(RESTRICTED_ACCESS, MATCHING_EVENT_DISCRIMINATORS,
                "", retailerDiscriminators(retailerIds));
    }

    private static ExplainAttributeResult withFullAccess(final String... retailerIds) {
        return new ExplainAttributeResult(FULL_ACCESS, MATCHING_EVENT_DISCRIMINATORS,
                "", retailerDiscriminators(retailerIds));
    }

    private static ExplainAttributeResult withNoAccess(final String... retailerIds) {
        return new ExplainAttributeResult(NO_ACCESS, MATCHING_EVENT_DISCRIMINATORS,
                "", retailerDiscriminators(retailerIds));
    }

    private static List<MatchingEventDiscriminator> retailerDiscriminators(final String... retailerIds) {
       return List.of(new MatchingEventDiscriminator("retailer_id",
               new HashSet<>(List.of(retailerIds))));
    }

    private static AuthorizationAttribute authAttr(final String type,
                                                   final String value) {
        return new SimpleAuthorizationAttribute(type, value);
    }
}
