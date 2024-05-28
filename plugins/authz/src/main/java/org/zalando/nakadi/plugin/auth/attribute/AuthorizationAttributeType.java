package org.zalando.nakadi.plugin.auth.attribute;

public class AuthorizationAttributeType {
    public static final String AUTH_ALL_ACCESS_TOKEN = "*";
    public static final String AUTH_TEAM = "team";
    public static final String AUTH_USER = "user";
    public static final String AUTH_SERVICE = "service";
    public static final String AUTH_BUSINESS_PARTNER = "business_partner";

    public static final String ASPD_DATA_CLASSIFICATION = "aspd-classification";
    public static final String EOS_NAME = "event_owner_selector.name";
    public static final String EOS_RETAILER_ID = "retailer_id"; // NOTE: this attribute is used for event-level access
}
