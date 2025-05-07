package org.zalando.nakadi.exceptions.runtime;

public class InvalidFilterLangException extends NakadiBaseException {
    private final String lang;

    public InvalidFilterLangException(final String lang) {
        this.lang = lang;
    }

    @Override
    public String getMessage() {
        return "Invalid value specified in ssf_lang: " + lang + ". Supported values are: sql_v1";
    }

}
