package org.zalando.nakadi.plugin.api.authz;

import java.util.Objects;
import java.util.Optional;

public class ExplainResourceResult {
    private final AuthorizationAttribute parentAuthAttribute;
    private final AuthorizationAttribute authAttribute;
    private final ExplainAttributeResult result;

    public ExplainResourceResult(final AuthorizationAttribute parentAuthAttribute,
                                 final AuthorizationAttribute authAttribute,
                                 final ExplainAttributeResult result) {
        this.parentAuthAttribute = parentAuthAttribute;
        this.authAttribute = authAttribute;
        this.result = result;
    }

    public AuthorizationAttribute getParentAuthAttribute() {
        return parentAuthAttribute;
    }

    public Optional<AuthorizationAttribute> getAuthAttribute() {
        return Optional.ofNullable(authAttribute);
    }

    public ExplainAttributeResult getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "ExplainResourceResult {" +
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
        if (!(o instanceof ExplainResourceResult)) {
            return false;
        }
        final ExplainResourceResult that = (ExplainResourceResult) o;
        return Objects.equals(getParentAuthAttribute(), that.getParentAuthAttribute()) &&
                Objects.equals(getAuthAttribute(), that.getAuthAttribute())
                && Objects.equals(getResult(), that.getResult());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getParentAuthAttribute(),
                getAuthAttribute(), getResult());
    }
}
