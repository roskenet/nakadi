package org.zalando.nakadi.model;

import org.zalando.nakadi.plugin.api.authz.ExplainResourceResult;

import java.util.List;

public class EventTypeAuthExplainResult {
    private final List<ExplainResourceResult> readers;

    public EventTypeAuthExplainResult(final List<ExplainResourceResult> readers) {
        this.readers = readers;
    }

    public List<ExplainResourceResult> getReaders() {
        return readers;
    }

    @Override
    public String toString() {
        return "EventTypeAuthExplainResult{" +
                "readers=" + readers +
                '}';
    }
}