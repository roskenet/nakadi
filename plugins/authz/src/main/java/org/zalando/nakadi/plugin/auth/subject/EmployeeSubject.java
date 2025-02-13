package org.zalando.nakadi.plugin.auth.subject;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.auth.ZalandoTeamService;
import org.zalando.nakadi.plugin.auth.attribute.SimpleAuthorizationAttribute;
import org.zalando.nakadi.plugin.auth.attribute.TeamAuthorizationAttribute;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.zalando.nakadi.plugin.api.authz.ResourceType.ADMIN_RESOURCE;
import static org.zalando.nakadi.plugin.api.authz.ResourceType.ALL_DATA_ACCESS_RESOURCE;

public class EmployeeSubject extends UidSubject {
    private static final Map<String, List<AuthorizationService.Operation>> ADMIN_USER_DENIED_OPERATIONS = Map.of(
            ADMIN_RESOURCE, List.of(AuthorizationService.Operation.WRITE, AuthorizationService.Operation.ADMIN),
            ALL_DATA_ACCESS_RESOURCE, List.of(AuthorizationService.Operation.READ)
    );

    private final boolean denyAdminOperations;
    private final ZalandoTeamService teamService;

    public EmployeeSubject(
            final boolean denyAdminOperations,
            final String uid,
            final Supplier<Set<String>> retailerIdsSupplier,
            final String type,
            final ZalandoTeamService teamService) {
        super(uid, retailerIdsSupplier, type);
        this.denyAdminOperations = denyAdminOperations;
        this.teamService = teamService;
    }

    @Override
    protected boolean isOperationAllowed(
            final String resourceType,
            final AuthorizationService.Operation operation,
            final Optional<List<AuthorizationAttribute>> attributes) {

        if (denyAdminOperations && isDeniedAdminOperation(operation, resourceType)) {
            return false;
        }

        final List<AuthorizationAttribute> allAttributes = attributes.stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        final List<AuthorizationAttribute> teamMembers = attributes.stream()
                .flatMap(Collection::stream)
                .filter(TeamAuthorizationAttribute::isTeamAuthorizationAttribute)
                .map(AuthorizationAttribute::getValue)
                .flatMap(team -> teamService.getTeamMembers(team).stream())
                .map(member -> new SimpleAuthorizationAttribute(type, member))
                .collect(Collectors.toList());

        allAttributes.addAll(teamMembers);

        return super.isOperationAllowed(resourceType, operation, Optional.of(allAttributes));
    }

    private boolean isDeniedAdminOperation(final AuthorizationService.Operation operation, final String resourceType) {
        final var deniedOperations = ADMIN_USER_DENIED_OPERATIONS.get(resourceType);
        return deniedOperations != null && deniedOperations.contains(operation);
    }
}
