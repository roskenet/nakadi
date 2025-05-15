package org.zalando.nakadi.exceptions.runtime;

public class MissingFilterException extends NakadiBaseException {

    @Override
    public String getMessage() {
        return "You specified ssf_lang but no ssf_expr argument.";
    }

}
