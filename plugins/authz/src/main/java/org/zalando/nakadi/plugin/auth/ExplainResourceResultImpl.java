package org.zalando.nakadi.plugin.auth;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.ExplainAttributeResult;
import org.zalando.nakadi.plugin.api.authz.ExplainResourceResult;

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
                equals(that.getParentAuthAttribute()) : that.getParentAuthAttribute() != null){
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
