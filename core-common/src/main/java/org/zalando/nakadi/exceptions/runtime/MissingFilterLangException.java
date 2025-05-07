package org.zalando.nakadi.exceptions.runtime;

public class MissingFilterLangException extends NakadiBaseException {

    @Override
    public String getMessage() {
        return "You specified ssf_expr but no ssf_lang argument. Supported values are: sql_v1";
    }

}
