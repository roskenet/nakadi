package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;
import java.util.Objects;

@Immutable
public class EvalFilterRequest {

    @NotNull
    private ObjectNode event;

    @NotNull
    private String ssfExpr;

    @NotNull
    private String ssfLang;

    public EvalFilterRequest(
            @JsonProperty("event") final ObjectNode event,
            @JsonProperty("ssf_expr") final String ssfExpr,
            @JsonProperty("ssf_lang") final String ssfLang) {
        this.event = event;
        this.ssfExpr = ssfExpr;
        this.ssfLang = ssfLang;
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
        return event.equals(that.event) && ssfExpr.equals(that.ssfExpr) && ssfLang.equals(that.ssfLang);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event, ssfExpr, ssfLang);
    }

    public ObjectNode getEvent() {
        return event;
    }

    public EvalFilterRequest setEvent(final ObjectNode event) {
        this.event = event;
        return this;
    }

    public String getSsfExpr() {
        return ssfExpr;
    }

    public EvalFilterRequest setSsfExpr(final String ssfExpr) {
        this.ssfExpr = ssfExpr;
        return this;
    }

    public String getSsfLang() {
        return ssfLang;
    }

    public EvalFilterRequest setSsfLang(final String ssfLang) {
        this.ssfLang = ssfLang;
        return this;
    }
}
