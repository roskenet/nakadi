package org.zalando.nakadi.plugin.api.authz;

public interface ExplainResourceResult {

    AuthorizationAttribute getParentAuthAttribute();

    AuthorizationAttribute getAuthAttribute();

    ExplainAttributeResult getResult();
}