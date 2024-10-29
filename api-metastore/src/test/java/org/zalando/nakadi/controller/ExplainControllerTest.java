package org.zalando.nakadi.controller;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.zalando.nakadi.config.OptInTeamsConfig;
import org.zalando.nakadi.controller.advice.ExplainExceptionHandler;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;
import org.zalando.nakadi.model.EventTypeAuthExplainRequest;
import org.zalando.nakadi.model.EventTypeAuthExplainResult;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.ExplainAttributeResult;
import org.zalando.nakadi.plugin.api.authz.ExplainResourceResult;
import org.zalando.nakadi.plugin.api.authz.MatchingEventDiscriminator;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.validation.EventTypeAnnotationsValidator;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.EventOwnerSelector;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.plugin.api.authz.AccessLevel.FULL_ACCESS;
import static org.zalando.nakadi.plugin.api.authz.ExplainAttributeResult.AccessRestrictionType.MATCHING_EVENT_DISCRIMINATORS;

public class ExplainControllerTest {

    private FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
    private AuthorizationService authorizationService = mock(AuthorizationService.class);
    private OptInTeamsConfig optInTeamsConfig = mock(OptInTeamsConfig.class);
    private ApplicationService applicationService = mock(ApplicationService.class);
    private final AuthorizationValidator authorizationValidator = mock(AuthorizationValidator.class);
    private EventTypeAnnotationsValidator validator;
    private MockMvc mockMvc;

    @Before
    public void setUp() {
        validator = new EventTypeAnnotationsValidator(featureToggleService,
                authorizationService,
                optInTeamsConfig,
                applicationService,
                Collections.emptyList());
        final ExplainController explainController = new ExplainController(validator,
                authorizationValidator);
        mockMvc = standaloneSetup(explainController)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setControllerAdvice(new NakadiProblemExceptionHandler(), new ExplainExceptionHandler())
                .build();
    }

    private ResultActions postExplanation(final String resource, final String content) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = post("/explanations" + resource).
                contentType(APPLICATION_JSON).content(
                        content);
        return mockMvc.perform(requestBuilder);
    }

    private ResultActions postEvenTypeAuthExplain(final EventTypeAuthExplainRequest request) throws Exception {
        final String content = TestUtils.OBJECT_MAPPER.writeValueAsString(request);
        return postExplanation("/event-type-auth", content);
    }

    @Test
    public void testAnnotationValidation() throws Exception {
        final List<AuthorizationAttribute> userAttrs = List.of(new ResourceAuthorizationAttribute("user", "foo"));
        final var authSection = new ResourceAuthorization(userAttrs, userAttrs, userAttrs);
        final var request = new EventTypeAuthExplainRequest(
                Map.of("compliance.zalando.org/aspd-classification", "invalid"),
                null, authSection);
        postEvenTypeAuthExplain(request)
                .andExpect(status().isOk())
                .andExpect(content()
                        .json(
                                "{\"error\": {\"message\": \"" +
                                "Annotation compliance.zalando.org/aspd-classification is not valid. " +
                                "Provided value: \\\"invalid\\\". " +
                                "Possible values are: \\\"none\\\" or \\\"aspd\\\" or \\\"mcf-aspd\\\"." +
                                "\"}}"
                        ));
    }

    @Test
    public void testEOSValidation() throws Exception {
        final List<AuthorizationAttribute> userAttrs = List.of(new ResourceAuthorizationAttribute("user", "foo"));
        final var authSection = new ResourceAuthorization(userAttrs, userAttrs, userAttrs);
        final var request = new EventTypeAuthExplainRequest(
                Map.of("compliance.zalando.org/aspd-classification", "aspd"),
                new EventOwnerSelector(EventOwnerSelector.Type.PATH, "retailer_id", "some_path"),
                authSection);
        postEvenTypeAuthExplain(request)
                .andExpect(status().isOk())
                .andExpect(content()
                        .json(
                                "{\"error\": {\"message\": \"" +
                                "\\\"aspd\\\" data classification with event_owner_selector is not allowed" +
                                "\"}}"
                        ));
    }

    @Test
    public void testValidResponse() throws Exception {
        final List<AuthorizationAttribute> userAttrs = List.of(new ResourceAuthorizationAttribute("user", "foo"));
        final var authSection = new ResourceAuthorization(userAttrs, userAttrs, userAttrs);

        final var request = new EventTypeAuthExplainRequest(
                Map.of("compliance.zalando.org/aspd-classification", "mcf-aspd"),
                new EventOwnerSelector(EventOwnerSelector.Type.PATH,
                        "retailer_id", "some_path"),
                authSection);
        final List<MatchingEventDiscriminator> discriminator = List.of(
                new MatchingEventDiscriminator("retailer_id",
                Set.of("retailer_1", "retailer_2")));
        final ExplainAttributeResult attrResult = new ExplainAttributeResult(
                FULL_ACCESS, MATCHING_EVENT_DISCRIMINATORS,
                "some reason", discriminator);
        final ExplainResourceResult result = new ExplainResourceResult(null, userAttrs.get(0), attrResult);

        when(authorizationValidator.explainAuthorization(any())).
                thenReturn(List.of(result));

        postEvenTypeAuthExplain(request)
                .andExpect(status().isOk())
                .andExpect(content().string(
                        TestUtils.OBJECT_MAPPER.
                        writeValueAsString(EventTypeAuthExplainResult.fromExplainResult(List.of(result)))));
    }


}
