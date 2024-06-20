package org.zalando.nakadi.plugin.api.authz;

import java.util.Objects;
import java.util.Optional;

public class ExplainResourceResult {
    private final AuthorizationAttribute targetAttribute;
    private final AuthorizationAttribute primaryAttribute;
    private final ExplainAttributeResult result;

    public ExplainResourceResult(final AuthorizationAttribute targetAttribute,
                                 final AuthorizationAttribute primaryAttribute,
                                 final ExplainAttributeResult result) {
        this.targetAttribute = targetAttribute;
        this.primaryAttribute = primaryAttribute;
        this.result = result;
    }

    public Optional<AuthorizationAttribute> getTargetAttribute() {
        return Optional.ofNullable(targetAttribute);
    }

    public AuthorizationAttribute getPrimaryAttribute() {
        return primaryAttribute;
    }

    public ExplainAttributeResult getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "ExplainResourceResult {" +
                "targetAttribute=" + targetAttribute +
                ", primaryAttribute=" + primaryAttribute +
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
        return Objects.equals(getTargetAttribute(), that.getTargetAttribute()) &&
                Objects.equals(getPrimaryAttribute(), that.getPrimaryAttribute())
                && Objects.equals(getResult(), that.getResult());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTargetAttribute(),
                getPrimaryAttribute(), getResult());
    }
}
