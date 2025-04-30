package org.zalando.nakadi.view;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;

public class EvalFilterRequest {

    private JsonNode event;
    private String filter;

    public JsonNode getEvent() {
        return event;
    }

    public EvalFilterRequest setEvent(final JsonNode event) {
        this.event = event;
        return this;
    }

    public String getFilter() {
        return filter;
    }

    public EvalFilterRequest setFilter(final String filter) {
        this.filter = filter;
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
        EvalFilterRequest that = (EvalFilterRequest) o;
        return Objects.equals(event, that.event) && Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event, filter);
    }

}
