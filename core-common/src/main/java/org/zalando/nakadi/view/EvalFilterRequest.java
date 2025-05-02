package org.zalando.nakadi.view;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

public class EvalFilterRequest {

    private ObjectNode event;
    private String filter;

    public ObjectNode getEvent() {
        return event;
    }

    public EvalFilterRequest setEvent(final ObjectNode event) {
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
        final EvalFilterRequest that = (EvalFilterRequest) o;
        return Objects.equals(event, that.event) && Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event, filter);
    }

}
