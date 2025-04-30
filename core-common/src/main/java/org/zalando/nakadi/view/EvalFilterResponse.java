package org.zalando.nakadi.view;

import java.util.Objects;

public class EvalFilterResponse {


    private String parsedFilter;
    private boolean result;

    public boolean isResult() {
        return result;
    }

    public EvalFilterResponse setResult(final boolean result) {
        this.result = result;
        return this;
    }

    public String getParsedFilter() {
        return parsedFilter;
    }

    public EvalFilterResponse setParsedFilter(final String parsedFilter) {
        this.parsedFilter = parsedFilter;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final EvalFilterResponse that = (EvalFilterResponse) o;
        return result == that.result && Objects.equals(parsedFilter, that.parsedFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parsedFilter, result);
    }
}
