package org.zalando.nakadi.plugin.auth.subject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationProperty;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;
import org.zalando.nakadi.plugin.auth.ResourceType;
import org.zalando.nakadi.plugin.auth.attribute.AuthorizationAttributeType;
import org.zalando.nakadi.plugin.auth.property.AuthorizationPropertyType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class AntitrustComplianceAuthorizationTest {

    @ParameterizedTest
    @MethodSource("getAuthorizationTests")
    public void testAuthorization(
            final String description,
            final List<AuthorizationProperty> properties,
            final Set<String> allowedRetailerIds,
            final boolean expectedResult
    ) {

        final Principal principal = new TestPrincipal(() -> allowedRetailerIds);

        final var result = principal.isAuthorized(
                ResourceType.EVENT_TYPE_RESOURCE,
                AuthorizationService.Operation.READ,
                Optional.ofNullable(Collections.emptyList()),
                properties);

        Assertions.assertEquals(expectedResult, result, description);
    }

    public static Stream<Arguments> getAuthorizationTests() {
        return Stream.of(
                Arguments.of(
                        "Backwards compatibility: no ASPD annotation -> allow access",
                        List.of(),
                        Set.of(),
                        true
                ),
                Arguments.of(
                        "ASPD annotation='none' -> allow non-conditional access",
                        List.of(property(AuthorizationPropertyType.ASPD_DATA_CLASSIFICATION, "none")),
                        Set.of(),
                        true
                ),

                Arguments.of(
                        "ASPD annotation='aspd', allowed retailer ids=none -> deny access",
                        List.of(property(AuthorizationPropertyType.ASPD_DATA_CLASSIFICATION, "aspd")),
                        Set.of(),
                        false
                ),
                Arguments.of(
                        "ASPD annotation='aspd', allowed retailer ids=some -> allow access",
                        List.of(property(AuthorizationPropertyType.ASPD_DATA_CLASSIFICATION, "aspd")),
                        Set.of("a-retailer-id"),
                        true
                ),

                Arguments.of(
                        "ASPD annotation='mcf-aspd', allowed retailer ids=none -> deny access",
                        List.of(property(AuthorizationPropertyType.ASPD_DATA_CLASSIFICATION, "mcf-aspd")),
                        Set.of(),
                        false
                ),
                Arguments.of(
                        "ASPD annotation='mcf-aspd', allowed retailer ids=some -> deny access",
                        List.of(property(AuthorizationPropertyType.ASPD_DATA_CLASSIFICATION, "mcf-aspd")),
                        Set.of("a-retailer-id"),
                        false
                ),
                Arguments.of(
                        "ASPD annotation='mcf-aspd', allowed retailer ids=all -> allow access",
                        List.of(property(AuthorizationPropertyType.ASPD_DATA_CLASSIFICATION, "mcf-aspd")),
                        Set.of("*"),
                        true
                ),
                Arguments.of(
                        "ASPD annotation='mcf-aspd', EOS='retailer_id', allowed retailer ids=some -> allow access",
                        List.of(property(AuthorizationPropertyType.ASPD_DATA_CLASSIFICATION, "mcf-aspd"),
                                property(AuthorizationPropertyType.EOS_NAME,
                                        AuthorizationAttributeType.EOS_RETAILER_ID)),
                        Set.of("a-retailer-id"),
                        true
                )
        );
    }

    private static AuthorizationProperty property(final String name, final String value) {
        return new AuthorizationPropertyImpl(name, value);
    }

    private static class AuthorizationPropertyImpl implements AuthorizationProperty {
        private final String name;
        private final String value;

        private AuthorizationPropertyImpl(final String name, final String value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    private static class TestPrincipal extends Principal {

        protected TestPrincipal(final Supplier<Set<String>> retailerIdsSupplier) {
            super("test-uid", retailerIdsSupplier);
        }

        @Override
        public boolean isExternal() {
            return false;
        }

        @Override
        protected boolean isOperationAllowed(
                final String resourceType,
                final AuthorizationService.Operation operation,
                final Optional<List<AuthorizationAttribute>> attributes) {
            return true;
        }

        @Override
        public Set<String> getBpids() throws PluginException {
            return Set.of();
        }

        @Override
        public String getName() {
            return "test-principal";
        }
    }
}
