package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

@Immutable
public class EvalFilterRequest {

    @NotNull
    private ObjectNode event;

    @NotNull
    private String ssfExpr;

    @NotNull
    private String ssfLang;

    public EvalFilterRequest(
            @JsonProperty(value = "event", required = true) final ObjectNode event,
            @JsonProperty(value = "ssf_expr", required = true) final String ssfExpr,
            @JsonProperty(value = "ssf_lang", required = true) final String ssfLang) {
        this.event = event;
        this.ssfExpr = ssfExpr;
        this.ssfLang = ssfLang;
    }

    public ObjectNode getEvent() {
        return event;
    }

    public String getSsfExpr() {
        return ssfExpr;
    }

    public String getSsfLang() {
        return ssfLang;
    }

}
