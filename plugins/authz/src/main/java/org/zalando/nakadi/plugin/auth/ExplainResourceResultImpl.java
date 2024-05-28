package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.ExplainAttributeResult;
import org.zalando.nakadi.plugin.api.authz.ExplainResourceResult;

import java.util.Objects;

public class ExplainResourceResultImpl implements ExplainResourceResult {
    private final AuthorizationAttribute parentAuthAttribute;
    private final AuthorizationAttribute authAttribute;
    private final ExplainAttributeResult result;

    public ExplainResourceResultImpl(final AuthorizationAttribute parentAuthAttribute,
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
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ExplainResourceResultImpl that = (ExplainResourceResultImpl) o;

        if (!Objects.equals(parentAuthAttribute, that.parentAuthAttribute)) {
            return false;
        }
        if (!authAttribute.equals(that.authAttribute)) {
            return false;
        }
        return result.equals(that.result);
    }

    @Override
    public int hashCode() {
        int result1 = parentAuthAttribute != null ? parentAuthAttribute.hashCode() : 0;
        result1 = 31 * result1 + authAttribute.hashCode();
        result1 = 31 * result1 + result.hashCode();
        return result1;
    }

    @Override
    public String toString() {
        return "ExplainResourceResultImpl{" +
                "parentAuthAttribute=" + parentAuthAttribute +
                ", authAttribute=" + authAttribute +
                ", result=" + result +
                '}';
    }
}
