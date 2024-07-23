package org.zalando.nakadi.webservice;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.specification.RequestSpecification;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.OK;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.utils.TestUtils.randomValidEventTypeName;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;

public class TestDataFilteringAT extends RealEnvironmentAT {

    private String eventTypeName;
    private String eventTypeBody;

    private String eventTypeNameBusiness;
    private String eventTypeBodyBusiness;
    private static String businessEvent1;
    private static String businessEvent2;
    private static String businessTestEvent1;
    private static String businessTestEvent2;
    private Subscription subscription;

    @Before
    public void beforeEach() throws IOException {
        eventTypeName = randomValidEventTypeName();
        eventTypeBody = getEventTypeJsonFromFile("sample-event-type.json", eventTypeName, owningApp);
        createEventType(eventTypeBody);

        businessEvent1 = getJsonFromResource("business_events/sample_business_event_1.json");
        businessEvent2 = getJsonFromResource("business_events/sample_business_event_2.json");
        businessTestEvent1 = getJsonFromResource("business_events/sample_business_event_3.json");
        businessTestEvent2 = getJsonFromResource("business_events/sample_business_event_4.json");
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
    public void testDefaultConfigurationWhenStreamParametersNotProvidedToGetEndpoint() throws IOException {
        // DEFAULT CONFIGURATION over GET
        final Set<String> expectedEIDsDefault = Set.of(
                TestUtils.getEventEid(new JSONObject(businessEvent1)),
                TestUtils.getEventEid(new JSONObject(businessEvent2))
        );
        testEventsFlow(expectedEIDsDefault, "", Optional.empty());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void testDefaultConfigurationWhenStreamParametersNotProvidedToPostEndpoint() throws IOException {
        // DEFAULT CONFIGURATION over POST
        final Set<String> expectedEIDsDefault = Set.of(
                TestUtils.getEventEid(new JSONObject(businessEvent1)),
                TestUtils.getEventEid(new JSONObject(businessEvent2)));
        testEventsFlow(expectedEIDsDefault, "", Optional.of("{}"));
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void testLiveAndTestConfigurationProvidedToGetEndpoint() throws IOException {
        // LIVE_AND_TEST CONFIGURATION over GET
        final Set<String> expectedEIDsDefault = Set.of(
                TestUtils.getEventEid(new JSONObject(businessEvent1)),
                TestUtils.getEventEid(new JSONObject(businessTestEvent1)),
                TestUtils.getEventEid(new JSONObject(businessEvent2)),
                TestUtils.getEventEid(new JSONObject(businessTestEvent2))
        );
        testEventsFlow(expectedEIDsDefault, "test_data_filter=LIVE_AND_TEST", Optional.empty());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void testLiveAndTestConfigurationProvidedToPostEndpoint() throws IOException {
        // LIVE_AND_TEST CONFIGURATION over POST
        final Set<String> expectedEIDsDefault = Set.of(
                TestUtils.getEventEid(new JSONObject(businessEvent1)),
                TestUtils.getEventEid(new JSONObject(businessTestEvent1)),
                TestUtils.getEventEid(new JSONObject(businessEvent2)),
                TestUtils.getEventEid(new JSONObject(businessTestEvent2))
        );
        testEventsFlow(expectedEIDsDefault, "", Optional.of("{\"test_data_filter\": \"LIVE_AND_TEST\"}"));
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void checkTestOnlyConfigurationProvidedToGetEndpoint() throws IOException {
        // TEST CONFIGURATION over GET
        final Set<String> expectedEIDsDefault = Set.of(
                TestUtils.getEventEid(new JSONObject(businessTestEvent1)),
                TestUtils.getEventEid(new JSONObject(businessTestEvent2))
        );
        testEventsFlow(expectedEIDsDefault, "test_data_filter=TEST", Optional.empty());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 1500000)
    public void checkTestOnlyConfigurationProvidedToPostEndpoint() throws IOException {
        // TEST CONFIGURATION over POST
        final Set<String> expectedEIDsDefault = Set.of(
                TestUtils.getEventEid(new JSONObject(businessTestEvent1)),
                TestUtils.getEventEid(new JSONObject(businessTestEvent2))
        );
        testEventsFlow(expectedEIDsDefault, "", Optional.of("{\"test_data_filter\": \"TEST\"}"));
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void testLiveOnlyConfigurationProvidedToGetEndpoint() throws IOException {
        // LIVE CONFIGURATION over GET
        final Set<String> expectedEIDsDefault = Set.of(
                TestUtils.getEventEid(new JSONObject(businessEvent1)),
                TestUtils.getEventEid(new JSONObject(businessEvent2))
        );
        testEventsFlow(expectedEIDsDefault, "test_data_filter=LIVE", Optional.empty());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 15000)
    public void testLiveOnlyConfigurationProvidedToPostEndpoint() throws IOException {
        // LIVE CONFIGURATION over POST
        final Set<String> expectedEIDsDefault = Set.of(
                TestUtils.getEventEid(new JSONObject(businessEvent1)),
                TestUtils.getEventEid(new JSONObject(businessEvent2)));
        testEventsFlow(expectedEIDsDefault, "", Optional.of("{\"test_data_filter\": \"LIVE\"}"));
    }

    private void testEventsFlow(
            final Set<String> expectedEventEIDs,
            final String getParams,
            final Optional<String> bodyPayload
    ) throws IOException {
        postEvents(eventTypeNameBusiness, businessEvent1, businessTestEvent1, businessEvent2, businessTestEvent2);

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
        jsonRequestSpec().body(body).when().post("/event-types").then().body(equalTo("")).statusCode(CREATED.value());
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
}
