package org.zalando.nakadi.plugin.auth.attribute;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;

public class TeamAuthorizationAttribute extends SimpleAuthorizationAttribute {

    public TeamAuthorizationAttribute(final String value) {
        super(AuthorizationAttributeType.AUTH_TEAM, value);
    }

    public static boolean isTeamAuthorizationAttribute(final AuthorizationAttribute attribute) {
        return AuthorizationAttributeType.AUTH_TEAM.equals(attribute.getDataType());
    }

}
