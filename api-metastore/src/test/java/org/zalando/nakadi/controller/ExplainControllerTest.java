package org.zalando.nakadi.controller;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.zalando.nakadi.controller.advice.ExplainExceptionHandler;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;
import org.zalando.nakadi.model.EventTypeAuthExplainRequest;
import org.zalando.nakadi.model.EventTypeAuthExplainResult;
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
import static org.zalando.nakadi.plugin.api.authz.ExplainAttributeResult.AccessLevel.FULL_ACCESS;
import static org.zalando.nakadi.plugin.api.authz.ExplainAttributeResult.AccessRestrictionType.MATCHING_EVENT_DISCRIMINATORS;

public class ExplainControllerTest {

    private FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
    private AuthorizationService authorizationService = mock(AuthorizationService.class);
    private final AuthorizationValidator authorizationValidator = mock(AuthorizationValidator.class);
    private EventTypeAnnotationsValidator validator;
    private MockMvc mockMvc;

    @Before
    public void setUp() throws Exception {
        validator = new EventTypeAnnotationsValidator(featureToggleService,
                authorizationService,
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
        postEvenTypeAuthExplain(request).andExpect(status().isUnprocessableEntity());
    }

    @Test
    public void testEOSValidation() throws Exception {
        final List<AuthorizationAttribute> userAttrs = List.of(new ResourceAuthorizationAttribute("user", "foo"));
        final var authSection = new ResourceAuthorization(userAttrs, userAttrs, userAttrs);
        final var request = new EventTypeAuthExplainRequest(
                Map.of("compliance.zalando.org/aspd-classification", "aspd"),
                new EventOwnerSelector(EventOwnerSelector.Type.PATH, "retailer_id", "some_path"),
                authSection);
        postEvenTypeAuthExplain(request).andExpect(status().isUnprocessableEntity());
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
                new MatchingEventDiscriminatorImpl("retailer_id",
                Set.of("retailer_1", "retailer_2")));
        final ExplainAttributeResult attrResult = new ExplainAttributeResultImpl(
                FULL_ACCESS, MATCHING_EVENT_DISCRIMINATORS,
                "some reason", discriminator);
        final ExplainResourceResult result = new ExplainResourceResultImpl(null, userAttrs.get(0), attrResult);

        when(authorizationValidator.explainReadAuthorization(any())).
                thenReturn(List.of(result));

        postEvenTypeAuthExplain(request).andExpect(status().is2xxSuccessful()).
                andExpect(content().string(
                        TestUtils.OBJECT_MAPPER.
                        writeValueAsString(new EventTypeAuthExplainResult(List.of(result)))));
    }

    private static class ExplainResourceResultImpl implements ExplainResourceResult {
        private final AuthorizationAttribute parentAuthAttribute;
        private final AuthorizationAttribute authAttribute;
        private final ExplainAttributeResult result;

        ExplainResourceResultImpl(final AuthorizationAttribute parentAuthAttribute,
                                         final AuthorizationAttribute authAttribute,
                                         final ExplainAttributeResult result) {
            this.parentAuthAttribute = parentAuthAttribute;
            this.authAttribute = authAttribute;
            this.result = result;
        }

        @Override
        public AuthorizationAttribute getParentAuthAttribute() {
            return parentAuthAttribute;
        }

        @Override
        public AuthorizationAttribute getAuthAttribute() {
            return authAttribute;
        }

        @Override
        public ExplainAttributeResult getResult() {
            return result;
        }

        @Override
        public String toString() {
            return "ExplainResourceResultImpl{" +
                    "parentAuthAttribute=" + parentAuthAttribute +
                    ", authAttribute=" + authAttribute +
                    ", result=" + result +
                    '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ExplainResourceResultImpl)) {
                return false;
            }

            final ExplainResourceResultImpl that = (ExplainResourceResultImpl) o;
            if (getParentAuthAttribute() != null ? !getParentAuthAttribute().
                    equals(that.getParentAuthAttribute()) : that.getParentAuthAttribute() != null) {
                return false;
            }
            if (!getAuthAttribute().equals(that.getAuthAttribute())) {
                return false;
            }
            return getResult().equals(that.getResult());
        }

        @Override
        public int hashCode() {
            int result1 = getParentAuthAttribute() != null ? getParentAuthAttribute().hashCode() : 0;
            result1 = 31 * result1 + getAuthAttribute().hashCode();
            result1 = 31 * result1 + getResult().hashCode();
            return result1;
        }
    }


    private static class ExplainAttributeResultImpl implements ExplainAttributeResult {
        private AccessLevel accessLevel;
        private AccessRestrictionType accessRestrictionType;
        private String reason;
        private List<MatchingEventDiscriminator> matchingEventDiscriminators;

        ExplainAttributeResultImpl(final AccessLevel accessLevel,
                                          final AccessRestrictionType accessRestrictionType,
                                          final String reason,
                                          final List<MatchingEventDiscriminator> matchingEventDiscriminators) {
            this.accessLevel = accessLevel;
            this.accessRestrictionType = accessRestrictionType;
            this.reason = reason;
            this.matchingEventDiscriminators = matchingEventDiscriminators;
        }

        @Override
        public AccessLevel getAccessLevel() {
            return accessLevel;
        }

        @Override
        public AccessRestrictionType getAccessRestrictionType() {
            return accessRestrictionType;
        }

        @Override
        public List<MatchingEventDiscriminator> getMatchingEventDiscriminators() {
            return matchingEventDiscriminators;
        }

        @Override
        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return "ExplainAttributeResultImpl{" +
                    "accessLevel=" + accessLevel +
                    ", accessRestrictionType=" + accessRestrictionType +
                    ", reason='" + reason + '\'' +
                    ", matchingEventDiscriminators=" + matchingEventDiscriminators +
                    '}';
        }


        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ExplainAttributeResultImpl)) {
                return false;
            }

            final ExplainAttributeResultImpl that = (ExplainAttributeResultImpl) o;

            if (getAccessLevel() != that.getAccessLevel()) {
                return false;
            }
            if (getAccessRestrictionType() != that.getAccessRestrictionType()) {
                return false;
            }
            return getMatchingEventDiscriminators().equals(that.getMatchingEventDiscriminators());
        }

        @Override
        public int hashCode() {
            int result = getAccessLevel().hashCode();
            result = 31 * result + getAccessRestrictionType().hashCode();
            result = 31 * result + getMatchingEventDiscriminators().hashCode();
            return result;
        }
    }

    private static class MatchingEventDiscriminatorImpl implements MatchingEventDiscriminator {
        private final String name;
        private final Set<String> values;

        MatchingEventDiscriminatorImpl(final String name,
                                              final Set<String> values) {
            this.name = name;
            this.values = values;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Set<String> getValues() {
            return values;
        }

        @Override
        public String toString() {
            return "MatchingEventDiscriminatorImpl{" +
                    "name='" + name + '\'' +
                    ", values=" + values +
                    '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MatchingEventDiscriminatorImpl)) {
                return false;
            }

            final MatchingEventDiscriminatorImpl that = (MatchingEventDiscriminatorImpl) o;

            if (!getName().equals(that.getName())) {
                return false;
            }
            return getValues().equals(that.getValues());
        }

        @Override
        public int hashCode() {
            int result = getName().hashCode();
            result = 31 * result + getValues().hashCode();
            return result;
        }
    }
}
