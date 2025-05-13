package org.zalando.nakadi.webservice;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.specification.RequestSpecification;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.OK;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.utils.TestUtils.randomValidEventTypeName;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;

public class FilteringAT extends RealEnvironmentAT {

    private String eventTypeName;
    private String eventTypeBody;

    private String eventTypeNameBusiness;
    private String eventTypeBodyBusiness;
    private static String event1;
    private static String event2;
    private static String event3;
    private static String event4;
    private Subscription subscription;

    @Before
    public void beforeEach() throws IOException {
        eventTypeName = randomValidEventTypeName();
        eventTypeBody = getEventTypeJsonFromFile("sample-event-type.json", eventTypeName, owningApp);
        createEventType(eventTypeBody);

        // each event N has field foo set to bar_N except for event 6 which is set to baz_6
        event1 = getJsonFromResource("business_events/sample_business_event_5.json");
        event2 = getJsonFromResource("business_events/sample_business_event_6.json");
        event3 = getJsonFromResource("business_events/sample_business_event_7.json");
        event4 = getJsonFromResource("business_events/sample_business_event_8.json");
        
        eventTypeNameBusiness = eventTypeName + ".business";
        eventTypeBodyBusiness = getEventTypeJsonFromFile("sample-event-type-business.json",
                eventTypeNameBusiness, owningApp);
        createEventType(eventTypeBodyBusiness);

        final SubscriptionBase subscriptionToCreate =
                RandomSubscriptionBuilder
                        .builder()
                        .withOwningApplication("stups_aruha-test-end2end-nakadi")
                        .withEventType(eventTypeNameBusiness)
                        .withStartFrom(BEGIN).buildSubscriptionBase();
        subscription = createSubscription(jsonRequestSpec(), subscriptionToCreate);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void testPost400() {
        // missing ssf_expr
        jsonRequestSpec()
                .body("{ \"ssf_lang\": \"sql_v1\" }")
                .when()
                .post("/subscriptions/" + subscription.getId() + "/events")
                .then()
                .statusCode(BAD_REQUEST.value());
        // missing ssf_lang
        jsonRequestSpec()
                .body("{ \"ssf_expr\": \"e.foo LIKE 'bar_%'\" }")
                .when()
                .post("/subscriptions/" + subscription.getId() + "/events")
                .then()
                .statusCode(BAD_REQUEST.value());
        // invalid ssf_lang
        jsonRequestSpec()
                .body("{ \"ssf_lang\": \"prolog_v1000\", \"ssf_expr\": \"e.foo LIKE 'bar_%'\" }")
                .when()
                .post("/subscriptions/" + subscription.getId() + "/events")
                .then()
                .statusCode(BAD_REQUEST.value());
        // malformed ssf_expr
        jsonRequestSpec()
                .body("{ \"ssf_lang\": \"sql_v1\", \"ssf_expr\": \"e.foo UNLIKE 'bar_%'\" }")
                .when()
                .post("/subscriptions/" + subscription.getId() + "/events")
                .then()
                .statusCode(BAD_REQUEST.value());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void testGet400() {
        // missing ssf_expr
        jsonRequestSpec()
                .when()
                .get("/subscriptions/" + subscription.getId() + "/events?ssf_lang=sql_v1")
                .then()
                .statusCode(BAD_REQUEST.value());
        // missing ssf_lang
        jsonRequestSpec()
                .body("{ \"ssf_expr\": \"e.foo LIKE 'bar_%'\" }")
                .when()
                .get("/subscriptions/" + subscription.getId() + "/events?ssf_expr=e.foo%20IS%20NULL")
                .then()
                .statusCode(BAD_REQUEST.value());
        // invalid ssf_lang
        jsonRequestSpec()
                .when()
                .get("/subscriptions/" + subscription.getId() + "/events?ssf_lang=prolog_v1000&ssf_expr=e.foo%20IS%20NULL")
                .then()
                .statusCode(BAD_REQUEST.value());
        // malformed ssf_expr
        jsonRequestSpec()
                .when()
                .get("/subscriptions/" + subscription.getId() + "/events?ssf_lang=prolog_v1000&ssf_expr=e.foo%20IS%20DULL")
                .then()
                .statusCode(BAD_REQUEST.value());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void testDefaultConfigurationWhenStreamParametersNotProvidedToGetEndpoint() throws IOException {
        // DEFAULT CONFIGURATION over GET
        final Set<String> expectedEIDsDefault = getEventEids(event1, event2, event3, event4);
        testEventsFlow(expectedEIDsDefault, "", Optional.empty());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void testDefaultConfigurationWhenStreamParametersNotProvidedToPostEndpoint() throws IOException {
        // DEFAULT CONFIGURATION over POST
        final Set<String> expectedEIDsDefault = getEventEids(event1, event2, event3, event4);
        testEventsFlow(expectedEIDsDefault, "", Optional.of("{}"));
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void testBasicFilterGetEndpoint() throws IOException {
        // only events that start with bar_
        final Set<String> expectedEIDsDefault = getEventEids(event1, event3, event4);
        final List<NameValuePair> parameters = List.of(
                new BasicNameValuePair("ssf_expr", "e.foo LIKE 'bar_%'"),
                new BasicNameValuePair("ssf_lang", "sql_v1"));
        final String getParams = URLEncodedUtils.format(parameters, "UTF-8");
        testEventsFlow(expectedEIDsDefault, getParams, Optional.empty());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void testLiveAndTestConfigurationProvidedToPostEndpoint() throws IOException {
        // LIVE_AND_TEST CONFIGURATION over POST
        final Set<String> expectedEIDsDefault = getEventEids(event1, event3, event4);
        final JSONObject body = new JSONObject();
        body.put("ssf_expr", "e.foo LIKE 'bar_%'");
        body.put("ssf_lang", "sql_v1");
        testEventsFlow(expectedEIDsDefault, "", Optional.of(body.toString()));
    }

    private void testEventsFlow(
            final Set<String> expectedEventEIDs,
            final String getParams,
            final Optional<String> bodyPayload
    ) throws IOException {
        postEvents(eventTypeNameBusiness, event1, event2, event3, event4);

        // read events
        // create client and wait till we receive all events
        final TestStreamingClient client = new TestStreamingClient(
                RestAssured.baseURI + ":" + RestAssured.port,
                subscription.getId(),
                getParams,
                oauthToken,
                bodyPayload
        ).start();
        waitFor(() -> assertThat(client.getJSONEvents(), hasSize(expectedEventEIDs.size())));
        assertThat(Set.copyOf(client.getJSONEIDs()), equalTo(expectedEventEIDs));
    }

    private void createEventType(final String body) {
        jsonRequestSpec().body(body).when().post("/event-types").then().statusCode(CREATED.value());
    }

    private void postEvents(final String eventTypeName, final String... events) {
        postEventsInternal(eventTypeName, events);
    }

    private void postEventsInternal(final String eventTypeName, final String[] events) {
        final String batch = "[" + String.join(",", events) + "]";
        jsonRequestSpec()
                .body(batch)
                .when()
                .post("/event-types/" + eventTypeName + "/events")
                .then()
                .body(equalTo(""))
                .statusCode(OK.value());
    }

    private void assertStatusCode(final String subscriptionId) {
        jsonRequestSpec()
                .body("{}")
                .when()
                .post("/subscriptions/" + subscriptionId + "/events")
                .then()
                .statusCode(OK.value());
    }

    private RequestSpecification jsonRequestSpec() {
        return requestSpec().header("accept", "application/json").contentType(ContentType.JSON);
    }

    private static String getJsonFromResource(final String resourceName) throws IOException {
        return Resources.toString(Resources.getResource(resourceName), Charsets.UTF_8);
    }

    public static String getEventTypeJsonFromFile(
            final String resourceName,
            final String eventTypeName,
            final String owningApp) throws IOException {
        final String json = getJsonFromResource(resourceName);
        return json.replace("NAME_PLACEHOLDER", eventTypeName).replace("OWNING_APP_PLACEHOLDER", owningApp);
    }

    private static Set<String> getEventEids(final String... events) {
        return Arrays.stream(events)
                .map(JSONObject::new)
                .map(TestUtils::getEventEid)
                .collect(Collectors.toSet());
    }

}
