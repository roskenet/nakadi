package org.zalando.nakadi.view;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;

public class EvalFilterResponse {

    private boolean result;

    public boolean isResult() {
        return result;
    }

    public EvalFilterResponse setResult(boolean result) {
        this.result = result;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EvalFilterResponse that = (EvalFilterResponse) o;
        return result == that.result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(result);
    }

}
