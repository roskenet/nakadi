package org.zalando.nakadi.view;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nonnull;
import java.util.Objects;

public class EvalFilterRequest {

    @Nonnull
    private ObjectNode event;

    @Nonnull
    @JsonProperty("ssf_expr")
    private String ssfExpr;

    @Nonnull
    @JsonProperty("ssf_lang")
    private String ssfLang;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EvalFilterRequest that = (EvalFilterRequest) o;
        return event.equals(that.event) && ssfExpr.equals(that.ssfExpr) && ssfLang.equals(that.ssfLang);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event, ssfExpr, ssfLang);
    }

    public ObjectNode getEvent() {
        return event;
    }

    public EvalFilterRequest setEvent(ObjectNode event) {
        this.event = event;
        return this;
    }

    public String getSsfExpr() {
        return ssfExpr;
    }

    public EvalFilterRequest setSsfExpr(String ssfExpr) {
        this.ssfExpr = ssfExpr;
        return this;
    }

    public String getSsfLang() {
        return ssfLang;
    }

    public EvalFilterRequest setSsfLang(String ssfLang) {
        this.ssfLang = ssfLang;
        return this;
    }
}
