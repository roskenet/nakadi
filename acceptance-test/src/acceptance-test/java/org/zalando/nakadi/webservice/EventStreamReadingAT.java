package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.TestDataFilter;
import org.zalando.nakadi.partitioning.PartitionStrategy;
import org.zalando.nakadi.repository.kafka.KafkaTestHelper;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.util.ThreadUtils;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.jayway.restassured.RestAssured.given;
import static java.text.MessageFormat.format;
import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.hasSize;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventTypeInNakadi;

public class EventStreamReadingAT extends BaseAT {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static final String TEST_PARTITION = "0";
    private static final int PARTITIONS_NUM = 8;
    private static final String SEPARATOR = "\n";

    // Dummy events to push to Kafka
    // {
    //   "metadata": {
    //     "eid": "d3b3b3b3-3b3b-3b3b-3b3b-3b3b3b3b3b3b",
    //     "occurred_at": "2016-06-14T13:00:00Z"
    //   },
    //   "foo": "bar_<N>"
    // }
    private final Function<Integer, String> dummyEventGenerator = i -> {
        final ObjectNode event = JSON_MAPPER.createObjectNode();
        final ObjectNode metadata = JSON_MAPPER.createObjectNode();
        event.set("metadata", metadata);
        metadata.put("eid", UUID.randomUUID().toString());
        metadata.put("event_type", eventType.getName());
        metadata.put("occurred_at", "2016-06-14T13:00:00Z");
        metadata.put("received_at", "2016-06-14T13:00:10Z");
        event.put("foo", "bar_" + i);
        return event.toString();
    };

    private static String streamEndpoint;
    private static String topicName;
    private static EventType eventType;
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
        xNakadiCursors = JSON_MAPPER.writeValueAsString(initialCursors);
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenPushFewEventsAndReadThenGetEventsInStream()
            throws ExecutionException, InterruptedException {

        // ARRANGE //
        // push events to one of the partitions
        final int eventsPushed = 2;
        kafkaHelper.writeMultipleMessageToPartition(TEST_PARTITION, topicName, dummyEventGenerator, eventsPushed);

        // ACT //
        final Response response = readEvents(Optional.empty(), Optional.empty());

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        Assert.assertThat(batches, hasSize(PARTITIONS_NUM));
        batches.forEach(this::validateBatchStructure);

        // find the batch where we expect to see the messages we pushed
        final Map<String, Object> batchToCheck = batches
                .stream()
                .filter(isForPartition(TEST_PARTITION))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Failed to find a partition in a stream"));

        // calculate the offset we expect to see in this batch in a stream
        final Cursor partitionCursor = kafkaInitialNextOffsets.stream()
                .filter(cursor -> TEST_PARTITION.equals(cursor.getPartition()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Failed to find cursor for needed partition"));
        final String expectedOffset = TestUtils.toTimelineOffset(Long.parseLong(partitionCursor.getOffset()) - 1 +
                eventsPushed);

        // check that batch has offset, partition and events number we expect
        validateEventsBatch(batchToCheck, TEST_PARTITION, expectedOffset, eventsPushed);
    }

    @Test(timeout = 10000)
    public void whenAcceptEncodingGzipReceiveCompressedStream()
            throws ExecutionException, InterruptedException {

        // ARRANGE //
        // push events to one of the partitions
        final int eventsPushed = 2;
        kafkaHelper.writeMultipleMessageToPartition(TEST_PARTITION, topicName, dummyEventGenerator, eventsPushed);

        // ACT //
        final Response response = RestAssured.given()
                .header(new Header("X-nakadi-cursors", xNakadiCursors))
                .header(new Header("Accept-Encoding", "gzip"))
                .param("batch_limit", "5")
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(streamEndpoint);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");
        response.then().header("Content-Encoding", "gzip");
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenPushedAmountOfEventsMoreThanBatchSizeAndReadThenGetEventsInMultipleBatches()
            throws ExecutionException, InterruptedException {

        // ARRANGE //
        // push events to one of the partitions so that they don't fit into one branch
        final int batchLimit = 5;
        final int eventsPushed = 8;
        kafkaHelper.writeMultipleMessageToPartition(TEST_PARTITION, topicName, dummyEventGenerator, eventsPushed);

        // ACT //
        final Response response = RestAssured.given()
                .header(new Header("X-nakadi-cursors", xNakadiCursors))
                .param("batch_limit", batchLimit)
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(streamEndpoint);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        // for partition with events we should get 2 batches
        Assert.assertThat(batches, hasSize(PARTITIONS_NUM + 1));
        batches.forEach(batch -> validateBatchStructure(batch));

        // find the batches where we expect to see the messages we pushed
        final List<Map<String, Object>> batchesToCheck = batches
                .stream()
                .filter(isForPartition(TEST_PARTITION))
                .collect(Collectors.toList());
        Assert.assertThat(batchesToCheck, hasSize(2));

        // calculate the offset we expect to see in this batch in a stream
        final Cursor partitionCursor = kafkaInitialNextOffsets.stream()
                .filter(cursor -> TEST_PARTITION.equals(cursor.getPartition()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Failed to find cursor for needed partition"));
        final String expectedOffset1 =
                TestUtils.toTimelineOffset(Long.parseLong(partitionCursor.getOffset()) - 1 + batchLimit);
        final String expectedOffset2 =
                TestUtils.toTimelineOffset(Long.parseLong(partitionCursor.getOffset()) - 1 + eventsPushed);

        // check that batches have offset, partition and events number we expect
        validateEventsBatch(batchesToCheck.get(0), TEST_PARTITION, expectedOffset1, batchLimit);
        validateEventsBatch(batchesToCheck.get(1), TEST_PARTITION, expectedOffset2, eventsPushed - batchLimit);
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenReadFromTheEndThenLatestOffsetsInStream() {

        // ACT //
        // just stream without X-nakadi-cursors; that should make nakadi to read from the very end
        final Response response = RestAssured.given()
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(streamEndpoint);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        Assert.assertThat(batches, hasSize(PARTITIONS_NUM));
        batches.forEach(batch -> validateBatchStructure(batch));

        // validate that the latest offsets in batches correspond to the newest offsets
        final Set<Cursor> offsets = batches
                .stream()
                .map(batch -> {
                    final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");
                    return new Cursor(cursor.get("partition"), cursor.get("offset"));
                })
                .collect(Collectors.toSet());
        Assert.assertThat(offsets, Matchers.equalTo(Sets.newHashSet(initialCursors)));
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenReachKeepAliveLimitThenStreamIsClosed() {
        // ACT //
        final int keepAliveLimit = 3;
        final Response response = RestAssured.given()
                .param("batch_flush_timeout", "1")
                .param("stream_keep_alive_limit", keepAliveLimit)
                .when()
                .get(streamEndpoint);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        Assert.assertThat(batches, hasSize(PARTITIONS_NUM * keepAliveLimit));
        batches.forEach(batch -> validateBatchStructure(batch));
    }

    @Test(timeout = 5000)
    public void whenGetEventsWithUnknownTopicThenTopicNotFound() {
        RestAssured.given()
                .when()
                .get(createStreamEndpointUrl("blah-topic"))
                .then()
                .statusCode(HttpStatus.NOT_FOUND.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo("topic not found"));
    }

    @Test(timeout = 5000)
    public void whenStreamLimitLowerThanBatchLimitThenUnprocessableEntity() {
        RestAssured.given()
                .param("batch_limit", "10")
                .param("stream_limit", "5")
                .when()
                .get(streamEndpoint)
                .then()
                .statusCode(HttpStatus.UNPROCESSABLE_ENTITY.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo("stream_limit can't be lower than batch_limit"));
    }

    @Test(timeout = 5000)
    public void whenStreamTimeoutLowerThanBatchTimeoutThenUnprocessableEntity() {
        RestAssured.given()
                .param("batch_timeout", "10")
                .param("stream_timeout", "5")
                .when()
                .get(streamEndpoint)
                .then()
                .statusCode(HttpStatus.UNPROCESSABLE_ENTITY.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo("stream_timeout can't be lower than batch_flush_timeout"));
    }

    @Test(timeout = 5000)
    public void whenIncorrectCursorsFormatThenBadRequest() {
        RestAssured.given()
                .header(new Header("X-nakadi-cursors", "this_is_definitely_not_a_json"))
                .when()
                .get(streamEndpoint)
                .then()
                .statusCode(HttpStatus.BAD_REQUEST.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo("incorrect syntax of X-nakadi-cursors header"));
    }

    @Test(timeout = 5000)
    public void whenCursorsWithInvalidPartitionThenPreconditionFailed() {
        RestAssured.given()
                .header(new Header("X-nakadi-cursors", "[{\"partition\":\"very_wrong_partition\",\"offset\":\"3\"}]"))
                .when()
                .get(streamEndpoint)
                .then()
                .statusCode(HttpStatus.PRECONDITION_FAILED.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo("non existing partition very_wrong_partition"));
    }

    @Test(timeout = 5000)
    public void whenCursorsWithInvalidOffsetThenPreconditionFailed() {
        RestAssured.given()
                .header(new Header("X-nakadi-cursors", "[{\"partition\":\"0\",\"offset\":\"invalid_offset\"}]"))
                .when()
                .get(streamEndpoint)
                .then()
                .statusCode(HttpStatus.PRECONDITION_FAILED.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo("invalid offset invalid_offset for partition 0"));
    }

    @Test(timeout = 5000)
    public void whenCursorsWithOffsetsInFutureThenPreconditionFailed() {
        final String futureOffset = "000000000111000000";
        final String partition = "0";
        RestAssured.given()
                .header(new Header("X-nakadi-cursors",
                        "[{\"partition\":\"" + partition + "\",\"offset\":\"" + futureOffset + "\"}]"))
                .when()
                .get(streamEndpoint)
                .then()
                .statusCode(HttpStatus.PRECONDITION_FAILED.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo(
                                String.format(
                                        "offset %s for partition %s event type %s is unavailable as offsets " +
                                                "are in the future.", futureOffset, partition, eventType.getName()
                                )
                        )
                );
    }

    @Test(timeout = 10000)
    public void whenReadEventsForBlockedConsumerThen403() throws Exception {
        readEvents(Optional.empty(), Optional.empty())
                .then()
                .statusCode(HttpStatus.OK.value());

        SettingsControllerAT.blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
        try {
            TestUtils.waitFor(() -> readEvents(Optional.empty(), Optional.empty())
                    .then()
                    .statusCode(403)
                    .body("detail", Matchers.equalTo("Application or event type is blocked")), 1000, 200);
        } finally {
            SettingsControllerAT.whitelist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
        }

        readEvents(Optional.empty(), Optional.empty())
                .then()
                .statusCode(HttpStatus.OK.value());
    }

    private Response readEvents(final Optional<TestDataFilter> testDataFilter, final Optional<String> ssfFilter) {
        final RequestSpecification builder = given()
                .header(new Header("X-nakadi-cursors", xNakadiCursors))
                .param("batch_limit", "5")
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2");

        testDataFilter.ifPresent(filter -> builder.param("test_data_filter", filter.toString()));
        ssfFilter.ifPresent(filter -> builder
                .param("ssf_expr", filter)
                .param("ssf_lang", "sql_v1"));

        return builder.when().get(streamEndpoint);
    }

    @Test(timeout = 10000)
    public void testSSFBadRequests() {
        // missing ssf_lang
        given()
                .param("ssf_expr", "foo LIKE 'bar_%'")
                .get(streamEndpoint)
                .then()
                .statusCode(400);
        // missing ssf_expr
        given()
                .param("ssf_lang", "sql_v1")
                .get(streamEndpoint)
                .then()
                .statusCode(400);
        // invalid ssf_lang
        given()
                .param("ssf_lang", "prolog_v1000")
                .param("ssf_expr", "foo LIKE 'bar_%'")
                .get(streamEndpoint)
                .then()
                .statusCode(400);
        // invalid ssf_expr
        given()
                .param("ssf_lang", "sql_v1")
                .param("ssf_expr", "foo LUKE 'bar_%'")
                .get(streamEndpoint)
                .then()
                .statusCode(400);
    }

    @Ignore
    @Test(timeout = 10000)
    public void whenExceedMaxConsumersNumThen429() throws IOException, InterruptedException, ExecutionException {
        final String etName = NakadiTestUtils.createEventType().getName();

        // try to create 8 consuming connections
        final List<CompletableFuture<HttpURLConnection>> connectionFutures = range(0, 8)
                .mapToObj(x -> createConsumingConnection(etName))
                .collect(Collectors.toList());

        Assert.assertThat("first 5 connections should be accepted",
                countStatusCode(connectionFutures, HttpStatus.OK.value()),
                Matchers.equalTo(5));

        Assert.assertThat("last 3 connections should be rejected",
                countStatusCode(connectionFutures, HttpStatus.TOO_MANY_REQUESTS.value()),
                Matchers.equalTo(3));

        // close one of open connections
        for (final CompletableFuture<HttpURLConnection> conn : connectionFutures) {
            if (conn.get().getResponseCode() == HttpStatus.OK.value()) {
                conn.get().disconnect();
                break;
            }
        }
        range(0, 15).forEach(value -> NakadiTestUtils.publishEvent(etName, "{\"foo\": \"bar\"}"));

        // wait for Nakadi to recognize thаt connection is closed
        ThreadUtils.sleep(1000);

        // try to create 3 more connections
        final List<CompletableFuture<HttpURLConnection>> moreConnectionFutures = range(0, 3)
                .mapToObj(x -> createConsumingConnection(etName))
                .collect(Collectors.toList());

        Assert.assertThat("one more connection should be accepted",
                countStatusCode(moreConnectionFutures, HttpStatus.OK.value()),
                Matchers.equalTo(1));

        Assert.assertThat("other 2 connections should be rejected as we had only one new free slot",
                countStatusCode(moreConnectionFutures, HttpStatus.TOO_MANY_REQUESTS.value()),
                Matchers.equalTo(2));
    }

    private int countStatusCode(final List<CompletableFuture<HttpURLConnection>> futures, final int statusCode) {
        return (int) futures.stream()
                .filter(future -> {
                    try {
                        return future.get(5, TimeUnit.SECONDS).getResponseCode() == statusCode;
                    } catch (Exception e) {
                        throw new AssertionError("Failed to get future result");
                    }
                })
                .count();
    }

    private CompletableFuture<HttpURLConnection> createConsumingConnection(final String etName) {
        final CompletableFuture<HttpURLConnection> future = new CompletableFuture<>();
        new Thread(() -> {
            try {
                final URL url = new URL(URL + createStreamEndpointUrl(etName));
                final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.getResponseCode(); // wait till the response code comes
                future.complete(conn);
            } catch (IOException e) {
                throw new AssertionError();
            }
        }).start();
        return future;
    }

    @Test(timeout = 10000)
    public void whenReadEventsConsumerIsBlocked() throws Exception {
        // blocking streaming client after 3 seconds
        new Thread(() -> {
            try {
                ThreadUtils.sleep(3000);
                SettingsControllerAT.blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }).start();
        try {
            // read events from the stream until we are blocked otherwise TestTimedOutException will be thrown and test
            // is considered to be failed
            RestAssured.given()
                    .header(new Header("X-nakadi-cursors", xNakadiCursors))
                    .param("batch_limit", "1")
                    .param("stream_timeout", "60")
                    .param("batch_flush_timeout", "10")
                    .when()
                    .get(streamEndpoint);
        } finally {
            SettingsControllerAT.whitelist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
        }
    }

    @Test(timeout = 10000)
    public void whenMemoryOverflowEventsDumped() throws IOException {
        // Create event type
        final EventType loadEt = EventTypeTestBuilder.builder()
                .defaultStatistic(new EventTypeStatistics(PARTITIONS_NUM, PARTITIONS_NUM))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(loadEt);

        // Publish events to event type, that are not fitting memory
        final String evt = "{\"foo\":\"barbarbar\"}";
        final int eventCount = 2 * (10000 / evt.length());
        NakadiTestUtils.publishEvents(loadEt.getName(), eventCount, i -> evt);

        // Configure streaming so it will:(more than 10s and batch_limit
        // - definitely wait for more than test timeout (10s)
        // - collect batch, which size is greater than events published to this event type
        final String url = RestAssured.baseURI + ":" + RestAssured.port + createStreamEndpointUrl(loadEt.getName())
                + "?batch_limit=" + (10 * eventCount)
                + "&stream_limit=" + (10 * eventCount)
                + "&batch_flush_timeout=11"
                + "&stream_timeout=11";
        final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        // Start from the begin.
        connection.setRequestProperty("X-Nakadi-Cursors",
                "[" + IntStream.range(0, PARTITIONS_NUM)
                        .mapToObj(i -> "{\"partition\": \"" + i + "\",\"offset\":\"begin\"}")
                        .collect(Collectors.joining(",")) + "]");
        Assert.assertEquals(HttpServletResponse.SC_OK, connection.getResponseCode());

        try (InputStream inputStream = connection.getInputStream()) {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));

            final String line = reader.readLine();
            // If we read at least one line, than it means, that we were able to read data before test timeout reached.
            Assert.assertNotNull(line);
        }
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenConsumeEventsDontReceiveEventsWithTestProjectId() {
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
        final Response response = readEvents(Optional.empty(), Optional.empty());

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

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenOptIntoTestDataReceiveEventsWithTestProjectId() {
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
        final Response response = readEvents(Optional.of(TestDataFilter.LIVE_AND_TEST), Optional.empty());

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();

        final List<JsonNode> batches = deserializeBatchesJsonNode(body);
        final Set<String> responseBars = batches.stream()
                .map(b -> extractBars(b))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        Assert.assertEquals(
                Set.of("bar_01", "bar_02", "bar_03"), // receive all the events
                responseBars
        );
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenUseFilterThenReceiveOnlyMatchingEvents() {
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
                        "\"occurred_at\":\"2024-10-10T15:42:03.746Z\"" +
                        "}," +
                        "\"foo\": \"baz_02\"" +
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
        final Response response = readEvents(Optional.empty(), Optional.of("foo LIKE 'bar_%'"));

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();

        final List<JsonNode> batches = deserializeBatchesJsonNode(body);
        final Set<String> responseBars = batches.stream()
                .map(b -> extractBars(b))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        Assert.assertEquals(
                Set.of("bar_01", "bar_03"), // receive all the events starting with bar_
                responseBars
        );
    }

    @Test(timeout = 15000)
    public void testConsumptionForTombstoneEvents() throws JsonProcessingException {
        final EventType eventType = createCompactedEventType(2);

        final BiConsumer<String, String> publishEvent = (id, value) -> {
            final String event = "{\"metadata\":" +
                    "{\"occurred_at\":\"2019-12-12T14:25:21Z\",\"eid\":\"f08976bc-9754-4eb2-a9b4-9c9c47d4ce64\"," +
                    "\"partition_compaction_key\":\"" + value + "\"}, " +
                    "\"part_field\": \"" + value + "\"," +
                    "\"id\": \"" + id + "\"}";
            NakadiTestUtils.publishEvent(eventType.getName(), event);
        };

        final Consumer<String> publishDelete = (value) -> {
            final String event = "{\"metadata\": " +
                    "{\"partition_compaction_key\":\"" + value + "\"},\"part_field\": \"" + value
                    + "\"}";
            NakadiTestUtils.deleteEvent(eventType.getName(), event);
        };

        // upon execution, we expect that event type will receive:
        // 4 events published (p0: [1, 3], p1:  [2, 4])
        // 4 events deleted (p0: [k1, k1], p1:  [k2, k2]))
        // 2 event published (p0: [5, 6] p1: [])
        // Consumer is expected to receive all 10 events and only once.

        publishEvent.accept("1", "k1");
        publishEvent.accept("2", "k2");
        publishEvent.accept("3", "k1");
        publishEvent.accept("4", "k2");

        publishDelete.accept("k1");
        publishDelete.accept("k2");
        publishDelete.accept("k1");
        publishDelete.accept("k2");

        publishEvent.accept("5", "k1");
        publishEvent.accept("6", "k1");

        final var startCursors = JSON_MAPPER.writeValueAsString(List.of(
                new Cursor("0", "001-0001--1"),
                new Cursor("1", "001-0001--1")));
        final Response response = RestAssured.given()
                .header(new Header("X-nakadi-cursors", startCursors))
                .param("batch_limit", "2")
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .param("receive_tombstones", "true")
                .when()
                .get(createStreamEndpointUrl(eventType.getName()));

        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");
        final List<Map<String, Object>> batches = deserializeBatches(response.print());
        final List<Map<String, Object>> p0Batches = batches
                .stream()
                .filter(isForPartition("0"))
                .filter(b -> hasBatchType("events").test(b))
                .collect(Collectors.toList());

        final List<Map<String, Object>> p1Batches = batches
                .stream()
                .filter(isForPartition("1"))
                .filter(b -> hasBatchType("events").test(b))
                .collect(Collectors.toList());


        // validation start
        assertThat(p0Batches, hasSize(3));
        assertThat(p1Batches, hasSize(2));

       // the first two batches should contain only events and no tombstones, 4 events
        validateEventsBatch(p0Batches.get(0), "0", "001-0001-000000000000000001", 2);
        validateEventsBatch(p1Batches.get(0), "1", "001-0001-000000000000000001", 2);

        // the next two batches should contain tombstones in the same events array, 4 events
        validateEventsBatch(p0Batches.get(1), "0", "001-0001-000000000000000003", 2);
        validateEventsBatch(p1Batches.get(1), "1", "001-0001-000000000000000003", 2);
        validateTombstoneEvents(p0Batches.get(1),2, "k1");
        validateTombstoneEvents(p1Batches.get(1),2, "k2");

        // the last batch should only exist for partition 0 and it should have multiple events, 2 events
        // this shows that we are able to read events normally after tombstones
        validateEventsBatch(p0Batches.get(2), "0", "001-0001-000000000000000005", 2);
    }

    @Test(timeout = 15000)
    public void testTombstoneEventsWithTestingInProduction() throws JsonProcessingException {
        final EventType eventType = createCompactedEventType(1);
        final AtomicBoolean testFilter = new AtomicBoolean(false);
        final BiConsumer<String, String> publishEvent = (id, value) -> {

            final String event = "{\"metadata\":" +
                    "{\"occurred_at\":\"2019-12-12T14:25:21Z\",\"eid\":\"f08976bc-9754-4eb2-a9b4-9c9c47d4ce64\"," +
                    (testFilter.get() ? "\"test_project_id\":\"beauty-pilot\"," : "") +
                    "\"partition_compaction_key\":\"" + value + "\"}, " +
                    "\"part_field\": \"" + value + "\"," +
                    "\"id\": \"" + id + "\"}";
            NakadiTestUtils.publishEvent(eventType.getName(), event);
        };

        final Consumer<String> publishDelete = (value) -> {
            final String event = "{\"metadata\": " +
                    "{\"partition_compaction_key\":\"" + value + "\"" +
                    (testFilter.get() ? ",\"test_project_id\":\"beauty-pilot\"" : "") +
                    "}," +
                    "\"part_field\": \"" + value
                    + "\"}";
            NakadiTestUtils.deleteEvent(eventType.getName(), event);
        };

        publishEvent.accept("1", "k1");
        testFilter.set(true);
        publishEvent.accept("2", "k2");
        publishDelete.accept("k2");


        final var startCursors = JSON_MAPPER.writeValueAsString(List.of(
                new Cursor("0", "001-0001--1")));
        final Response responseAll = RestAssured.given()
                .header(new Header("X-nakadi-cursors", startCursors))
                .param("batch_limit", "2")
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .param("receive_tombstones", "true")
                .param("test_data_filter", "LIVE")
                .when()
                .get(createStreamEndpointUrl(eventType.getName()));

        final Response responseTest = RestAssured.given()
                .header(new Header("X-nakadi-cursors", startCursors))
                .param("batch_limit", "2")
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .param("receive_tombstones", "true")
                .param("test_data_filter", "TEST")
                .when()
                .get(createStreamEndpointUrl(eventType.getName()));

        final List<Map<String, Object>> testLiveBatches =  deserializeBatches(responseAll.print())
                .stream()
                .filter(b -> hasBatchType("events").test(b))
                .collect(Collectors.toList());

        final List<Map<String, Object>> testBatches = deserializeBatches(responseTest.print())
                .stream()
                .filter(b -> hasBatchType("events").test(b))
                .collect(Collectors.toList());

        assertThat(testLiveBatches, hasSize(1));
        assertThat(testBatches, hasSize(1));

        // batches for live filter receive no tombstone records for test filter
        validateEventsBatch(testLiveBatches.get(0), "0", "001-0001-000000000000000000", 1);
        validateTombstoneEvents(testLiveBatches.get(0), 0, "ignore");

        // batches for test filter receive tombstone records for test filter
        validateEventsBatch(testBatches.get(0), "0", "001-0001-000000000000000002", 2);
        validateTombstoneEvents(testBatches.get(0), 1, "k2");
    }

    private static EventType createCompactedEventType(final int partitions) throws JsonProcessingException {
        final EventType eventType = EventTypeTestBuilder.builder()
                .defaultStatistic(new EventTypeStatistics(partitions, partitions))
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .partitionStrategy(PartitionStrategy.HASH_STRATEGY)
                .partitionKeyFields(Collections.singletonList("part_field"))
                .category(EventCategory.BUSINESS)
                .enrichmentStrategies(Lists.newArrayList(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .schema("{\"type\": \"object\",\"properties\": " +
                        "{\"part_field\": {\"type\": \"string\"}, \"id\": {\"type\": \"string\"}}" +
                        ",\"required\": [\"part_field\"]}")
                .build();
        createEventTypeInNakadi(eventType);
        return eventType;
    }


    private static String createStreamEndpointUrl(final String eventType) {
        return format("/event-types/{0}/events", eventType);
    }

    @SuppressWarnings("unchecked")
    private Predicate<Map<String, Object>> isForPartition(final String partition) {
        return batch -> {
            final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");
            return partition.equals(cursor.get("partition"));
        };
    }

    private Predicate<Map<String, Object>> hasBatchType(final String batchType) {
        return batch -> batch.get(batchType) != null;
    }

    private List<Map<String, Object>> deserializeBatches(final String body) {
        return Arrays
                .stream(body.split(SEPARATOR))
                .map(batch -> {
                    try {
                        return JSON_MAPPER.readValue(batch,
                                new TypeReference<HashMap<String, Object>>() {
                                });
                    } catch (IOException e) {
                        e.printStackTrace();
                        Assert.fail("Could not deserialize response from streaming endpoint");
                        return null;
                    }
                })
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private void validateBatchStructure(final Map<String, Object> batch) {
        Assert.assertThat(batch, Matchers.hasKey("cursor"));
        final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");

        Assert.assertThat(cursor, Matchers.hasKey("partition"));
        Assert.assertThat(cursor, Matchers.hasKey("offset"));

        if (batch.containsKey("events")) {
            final List<Map<String, Object>> events = (List<Map<String, Object>>) batch.get("events");
            events.forEach(event -> {
                Assert.assertThat((String) event.get("foo"), Matchers.startsWith("bar_"));
            });
        }
    }

    @SuppressWarnings("unchecked")
    private void validateBatch(final Map<String, Object> batch, final String expectedPartition,
                               final String expectedOffset, final int expectedEventNum,
                               final String batchType) {
        final Set<String> validRootKeys = Set.of("cursor", batchType);
        final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");
        Assert.assertThat(cursor.get("partition"), Matchers.equalTo(expectedPartition));
        Assert.assertThat(cursor.get("offset"), Matchers.equalTo(expectedOffset));

        if (batch.containsKey(batchType)) {
            final List<String> events = (List<String>) batch.get(batchType);
            Assert.assertThat(events.size(), Matchers.equalTo(expectedEventNum));
        } else {
            Assert.assertThat(0, Matchers.equalTo(expectedEventNum));
        }

       batch.keySet().stream().filter(key -> !validRootKeys.contains(key))
                .forEach(key -> Assert.fail("Unexpected key in batch: " + key));
   }

    @SuppressWarnings("unchecked")
    private void validateEventsBatch(final Map<String, Object> batch, final String expectedPartition,
                                     final String expectedOffset, final int expectedEventNum) {
        validateBatch(batch, expectedPartition, expectedOffset, expectedEventNum, "events");
    }

    @SuppressWarnings("unchecked")
    private void validateTombstoneEvents(final Map<String, Object> batch,
                                         final int expectedEventNum,
                                         final String expectedKey) {
        final List<Map<String, Object>> events = (List<Map<String, Object>>) batch.get("events");
        final List<Map<String, Object>> tombstoneEvents = events.stream().filter(event -> {
                     final var metadata = (Map<String, Object>) event.get("metadata");
                     return metadata.get("is_tombstone") != null && metadata.get("is_tombstone").equals(true);
                }).collect(Collectors.toList());

        Assert.assertThat(tombstoneEvents, hasSize(expectedEventNum));
        for (final Map<String, Object> event : tombstoneEvents) {
            final Map<String, String> metadata = (Map<String, String>) event.get("metadata");
            Assert.assertThat(metadata.get("partition_compaction_key"), equalTo(expectedKey));
            Assert.assertThat(metadata.get("event_type"), notNullValue());
        }
    }

    @SuppressWarnings("unchecked")
    private List<JsonNode> deserializeBatchesJsonNode(final String body) {
        return Arrays
                .stream(body.split(SEPARATOR))
                .map(batch -> {
                    try {
                        return JSON_MAPPER.readTree(batch);
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
