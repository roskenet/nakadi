package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.repository.kafka.KafkaTestHelper;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.given;
import static java.text.MessageFormat.format;

public class BusinessEventStreamReadingAT extends BaseAT {

    private static final int PARTITIONS_NUM = 8;
    private static final String SEPARATOR = "\n";

    private static String streamEndpoint;
    private static String topicName;
    private static EventType eventType;

    private final ObjectMapper jsonMapper = new ObjectMapper();
    private KafkaTestHelper kafkaHelper;
    private String xNakadiCursors;
    private List<Cursor> initialCursors;
    private List<Cursor> kafkaInitialNextOffsets;

    @BeforeClass
    public static void setupClass() throws JsonProcessingException {
        eventType = EventTypeTestBuilder.builder()
                .category(EventCategory.BUSINESS)
                .enrichmentStrategies(Arrays.asList(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .defaultStatistic(new EventTypeStatistics(PARTITIONS_NUM, PARTITIONS_NUM))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(eventType);
        streamEndpoint = createStreamEndpointUrl(eventType.getName());
        // expect only one timeline, because we just created event type
        topicName = BaseAT.TIMELINE_REPOSITORY.listTimelinesOrdered(eventType.getName()).get(0).getTopic();
    }

    @Before
    public void setUp() throws JsonProcessingException {
        kafkaHelper = new KafkaTestHelper(KAFKA_URL);
        initialCursors = kafkaHelper.getOffsetsToReadFromLatest(topicName);
        kafkaInitialNextOffsets = kafkaHelper.getNextOffsets(topicName);
        xNakadiCursors = jsonMapper.writeValueAsString(initialCursors);
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenConsumeEventsDontReceiveTestEvents() {
        // ARRANGE //
        // push events to one of the partitions
        given()
                .body("[" +
                        "{" +
                            "\"metadata\":{" +
                                "\"eid\":\"9cd00c47-b792-4fc8-bb1b-317f04e3a2a0\"," +
                                "\"occurred_at\":\"2024-10-10T15:42:03.746Z\"" +
                            "}," +
                            "\"foo\": \"bar_01\"" +
                        "}," +
                        "{" +
                            "\"metadata\":{" +
                                "\"eid\":\"9cd00c47-b792-4fc8-bb1b-317f04e3a2a1\"," +
                                "\"occurred_at\":\"2024-10-10T15:42:03.746Z\"," +
                                "\"test_project_id\":\"beauty-pilot\"" +
                            "}," +
                            "\"foo\": \"bar_02\"" +
                        "}," +
                        "{" +
                            "\"metadata\":{" +
                                "\"eid\":\"9cd00c47-b792-4fc8-bb1b-317f04e3a2a2\"," +
                                "\"occurred_at\":\"2024-10-10T15:42:03.746Z\"" +
                            "}," +
                            "\"foo\": \"bar_03\"" +
                        "}" +
                      "]")
                .contentType(ContentType.JSON)
                .post(MessageFormat.format("/event-types/{0}/events", eventType.getName()))
                .then()
                .statusCode(200);

        // ACT //
        final Response response = readEvents();

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();

        final List<JsonNode> batches = deserializeBatchesJsonNode(body);
        final Set<String> responseBars = batches.stream()
                .map(b -> extractBars(b))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        Assert.assertEquals(
                Set.of("bar_01", "bar_03"), // notice bar_02 got filtered out
                responseBars
        );
    }

    private Response readEvents() {
        return RestAssured.given()
                .header(new Header("X-nakadi-cursors", xNakadiCursors))
                .param("batch_limit", "5")
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(streamEndpoint);
    }

    private static String createStreamEndpointUrl(final String eventType) {
        return format("/event-types/{0}/events", eventType);
    }

    @SuppressWarnings("unchecked")
    private List<JsonNode> deserializeBatchesJsonNode(final String body) {
        return Arrays
                .stream(body.split(SEPARATOR))
                .map(batch -> {
                    try {
                        return jsonMapper.readTree(batch);
                    } catch (IOException e) {
                        e.printStackTrace();
                        Assert.fail("Could not deserialize response from streaming endpoint");
                        return null;
                    }
                })
                .collect(Collectors.toList());
    }

    private Set<String> extractBars(final JsonNode batch) {
        final Set<String> bars = Sets.newHashSet();
        Optional
                .ofNullable(batch.get("events"))
                .map(events -> events.elements())
                .orElse(Collections.emptyIterator())
                .forEachRemaining(e -> bars.add(e.get("foo").asText()));
        return bars;
    }


}
