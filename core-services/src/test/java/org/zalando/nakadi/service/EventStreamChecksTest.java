package org.zalando.nakadi.service;

import org.junit.jupiter.api.Test;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.cache.SubscriptionCache;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.kafka.KafkaRecordDeserializer;
import org.zalando.nakadi.utils.TestUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.domain.HeaderTag.DEBUG_PUBLISHER_TOPIC_ID;

public class EventStreamChecksTest {

    private final EventTypeCache eventTypeCache = mock(EventTypeCache.class);
    private final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
    private final KafkaRecordDeserializer deserializer = mock(KafkaRecordDeserializer.class);

    private final EventStreamChecks eventStreamChecks = new EventStreamChecks(
            mock(BlacklistService.class),
            mock(AuthorizationService.class),
            deserializer,
            mock(SubscriptionCache.class),
            featureToggleService,
            eventTypeCache
    );

    private ConsumedEvent newConsumedEvent(final byte[] event) {
        final Timeline timeline = TestUtils.buildTimeline("correct.event-type");
        final NakadiCursor position = NakadiCursor.of(timeline, "0", "0");
        return new ConsumedEvent(
                event,
                position,
                0,
                new EventOwnerHeader("unit", "Nakadi"),
                Map.of(DEBUG_PUBLISHER_TOPIC_ID, "publisher"),
                Optional.empty()
        );
    }

    @Test
    public void testShouldSkipMisplacedEventWhenEventTypeDoesntMatches() {

        final byte[] event = ("{\"metadata\": {"
                + "\"event_type\": \"this.is-wrong\","
                + "\"occurred_at\": \"2024-10-10T15:42:03.746Z\","
                + "\"received_at\": \"2024-10-10T15:42:03.747Z\""
                + " }}").getBytes();

        final EventType mockEventType = mock(EventType.class);
        when(eventTypeCache.getEventType("correct.event-type")).thenReturn(mockEventType);
        when(mockEventType.getCategory()).thenReturn(EventCategory.BUSINESS);
        when(featureToggleService.isFeatureEnabled(Feature.SKIP_MISPLACED_EVENTS)).thenReturn(Boolean.TRUE);

        final boolean result = eventStreamChecks.shouldSkipMisplacedEvent(newConsumedEvent(event));

        assertTrue(result, "Event should be misplaced because event type doesn't match");
    }

    @Test
    public void testShouldSkipMisplacedEventWhenEventTypeDoesntMatchesButFlagisFalse() {

        final byte[] event = ("{\"metadata\": {"
                + "\"event_type\": \"this.is-wrong\","
                + "\"occurred_at\": \"2024-10-10T15:42:03.746Z\","
                + "\"received_at\": \"2024-10-10T15:42:03.747Z\""
                + " }}").getBytes();

        final EventType mockEventType = mock(EventType.class);
        when(eventTypeCache.getEventType("correct.event-type")).thenReturn(mockEventType);
        when(mockEventType.getCategory()).thenReturn(EventCategory.BUSINESS);
        when(featureToggleService.isFeatureEnabled(Feature.SKIP_MISPLACED_EVENTS)).thenReturn(Boolean.FALSE);

        final boolean result = eventStreamChecks.shouldSkipMisplacedEvent(newConsumedEvent(event));

        assertFalse(result, "Event should not be misplaced because event type doesn't match but flag is false");
    }

    @Test
    public void testShouldSkipMisplacedEventWhenDeserializerThrowsIOException() throws IOException {
        final byte[] event = ("{\"metadata\": {"
                + "\"event_type\": \"correct.event-type\","
                + "\"occurred_at\": \"2024-10-10T15:42:03.746Z\","
                + "\"received_at\": \"2024-10-10T15:42:03.747Z\""
                + "}}").getBytes();

        final EventType mockEventType = mock(EventType.class);

        when(eventTypeCache.getEventType("correct.event-type")).thenReturn(mockEventType);
        when(mockEventType.getCategory()).thenReturn(EventCategory.BUSINESS);
        when(featureToggleService.isFeatureEnabled(Feature.SKIP_MISPLACED_EVENTS)).thenReturn(Boolean.TRUE);
        when(deserializer.getEventTypeName(event)).thenThrow(new IOException("Failed to deserialize"));

        final Exception exception = assertThrows(NakadiRuntimeException.class, () ->
                eventStreamChecks.shouldSkipMisplacedEvent(newConsumedEvent(event))
        );

        assertEquals(
                "Failed to parse metadata to check for misplaced event in" +
                        " 'correct.event-type' at position T(correct.event-type:0:NULL)-P(0)-O(0)",
                exception.getMessage()
        );
        assertEquals("Failed to deserialize", exception.getCause().getMessage());
    }

    @Test
    public void testIsMisplacedEventWhenCategoryShouldSkipUndefined() {
        final byte[] event = ("{\"metadata\": {"
                + "\"event_type\": \"correct.event-type\","
                + "\"occurred_at\": \"2024-10-10T15:42:03.746Z\","
                + "\"received_at\": \"2024-10-10T15:42:03.747Z\""
                + "}}").getBytes();

        final EventType mockEventType = mock(EventType.class);
        when(eventTypeCache.getEventType("correct.event-type")).thenReturn(mockEventType);
        when(featureToggleService.isFeatureEnabled(Feature.SKIP_MISPLACED_EVENTS)).thenReturn(Boolean.TRUE);
        when(mockEventType.getCategory()).thenReturn(EventCategory.UNDEFINED);

        final boolean result = eventStreamChecks.shouldSkipMisplacedEvent(newConsumedEvent(event));

        assertFalse(result, "Event should not be misplaced when category is UNDEFINED");
    }

    @Test
    public void testShouldSkipMisplacedEventWhenEventTypeMatchesCorrectly() throws IOException {
        final byte[] event = ("{\"metadata\": {"
                + "\"event_type\": \"correct.event-type\","
                + "\"occurred_at\": \"2024-10-10T15:42:03.746Z\","
                + "\"received_at\": \"2024-10-10T15:42:03.747Z\""
                + "}}").getBytes();

        final EventType mockEventType = mock(EventType.class);
        when(eventTypeCache.getEventType("correct.event-type")).thenReturn(mockEventType);
        when(featureToggleService.isFeatureEnabled(Feature.SKIP_MISPLACED_EVENTS)).thenReturn(Boolean.TRUE);
        when(mockEventType.getCategory()).thenReturn(EventCategory.BUSINESS);

        when(deserializer.getEventTypeName(event)).thenReturn("correct.event-type");

        final boolean result = eventStreamChecks.shouldSkipMisplacedEvent(newConsumedEvent(event));

        assertFalse(result, "Event should not be misplaced when event type matches correctly");
    }
}