package org.zalando.nakadi.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.utils.TestUtils;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RepartitioningServiceTest {

    private EventTypeRepository eventTypeRepository;
    private EventTypeCache eventTypeCache;
    private TimelineService timelineService;
    private SubscriptionDbRepository subscriptionRepository;
    private SubscriptionClientFactory subscriptionClientFactory;
    private NakadiSettings nakadiSettings;
    private CursorConverter cursorConverter;
    private TimelineSync timelineSync;
    private NakadiAuditLogPublisher auditLogPublisher;
    private RepartitioningService repartitioningService;
    private TopicRepository topicRepository;
    private final String dlqEventTypeName = "dlq-event";

    @BeforeEach
    public void setup() {
        eventTypeRepository = mock(EventTypeRepository.class);
        eventTypeCache = mock(EventTypeCache.class);
        timelineService = mock(TimelineService.class);
        subscriptionRepository = mock(SubscriptionDbRepository.class);
        subscriptionClientFactory = mock(SubscriptionClientFactory.class);
        nakadiSettings = mock(NakadiSettings.class);
        cursorConverter = mock(CursorConverter.class);
        timelineSync = mock(TimelineSync.class);
        auditLogPublisher = mock(NakadiAuditLogPublisher.class);
        topicRepository = mock(TopicRepository.class);
        repartitioningService = new RepartitioningService(
                eventTypeRepository,
                eventTypeCache,
                timelineService,
                subscriptionRepository,
                subscriptionClientFactory,
                nakadiSettings,
                cursorConverter,
                timelineSync,
                dlqEventTypeName,
                auditLogPublisher
        );
    }

    @Test
    public void shouldThrowExceptionIfEventTypeIsDeadLetterQueue() {
        // Given
        final String eventTypeName = dlqEventTypeName;
        final int partitions = 2;

        // When And Then
        final InvalidEventTypeException invalidEventTypeException = assertThrows(InvalidEventTypeException.class,
                () -> repartitioningService.repartition(eventTypeName, partitions));
        assertEquals(invalidEventTypeException.getMessage(),
                "Repartitioning " + eventTypeName + " event type is not supported");
    }

    @Test
    public void shouldThrowExceptionWhenPartitionsIsGreaterThanMaxPartitionCount() {
        // Given
        final String eventTypeName = "test-event-1";
        final int partitions = 20;
        final int maxPartitions = 5;
        when(nakadiSettings.getMaxTopicPartitionCount()).thenReturn(maxPartitions);

        // When and Then
        final InvalidEventTypeException invalidEventTypeException = assertThrows(InvalidEventTypeException.class,
                () -> repartitioningService.repartition(eventTypeName, partitions));
        assertEquals(invalidEventTypeException.getMessage(),
                "Number of partitions should not be more than " + maxPartitions);
    }

    @Test
    public void shouldThrowExceptionIfNumberOfPartitionsIsLessThanCurrentPartitions() {
        // Given
        final int partitions = 2;
        final int maxPartitions = 20;
        final EventType eventType = TestUtils.buildDefaultEventType();
        final Timeline mockTimeline = mock(Timeline.class);
        when(nakadiSettings.getMaxTopicPartitionCount()).thenReturn(maxPartitions);
        when(eventTypeRepository.findByName(any())).thenReturn(eventType);
        when(timelineService.getTopicRepository(eventType)).thenReturn(topicRepository);
        when(timelineService.getActiveTimeline(eventType)).thenReturn(mockTimeline);
        when(mockTimeline.getTopic()).thenReturn("topic-id");
        when(topicRepository.listPartitionNames(anyString())).thenReturn(List.of("a", "b", "c"));

        // When and Then
        final InvalidEventTypeException invalidEventTypeException = assertThrows(InvalidEventTypeException.class,
                () -> repartitioningService.repartition(eventType.getName(), partitions));
        assertEquals(invalidEventTypeException.getMessage(),
                "Number of partitions can not decrease");
    }

    @Test
    public void shouldRepartitionEventTypeAndPublishAuditEvents() {
        // Given
        final int desiredPartitions = 5;
        final int currentPartitions = 3;
        final int maxPartitions = 20;
        final EventType eventType = TestUtils.buildDefaultEventType();
        eventType.setDefaultStatistic(new EventTypeStatistics(currentPartitions, currentPartitions));

        final Timeline mockTimeline = mock(Timeline.class);
        when(nakadiSettings.getMaxTopicPartitionCount()).thenReturn(maxPartitions);
        when(eventTypeRepository.findByName(any())).thenReturn(eventType);
        when(timelineService.getTopicRepository(eventType)).thenReturn(topicRepository);
        when(timelineService.getActiveTimeline(eventType)).thenReturn(mockTimeline);
        when(mockTimeline.getTopic()).thenReturn("topic-id");
        when(topicRepository.listPartitionNames(anyString())).thenReturn(List.of("a", "b", "c"));
        when(subscriptionRepository.listAllSubscriptionsFor(any())).thenReturn(List.of());
        final ArgumentCaptor<Optional<Object>> oldEventCaptor = ArgumentCaptor.forClass((Class) Optional.class);
        final ArgumentCaptor<Optional<Object>> newEventCaptor = ArgumentCaptor.forClass((Class) Optional.class);

        // When
        repartitioningService.repartition(eventType.getName(), desiredPartitions);

        // Then
        verify(auditLogPublisher, times(1))
                .publish(
                        oldEventCaptor.capture(),
                        newEventCaptor.capture(),
                        Mockito.eq(NakadiAuditLogPublisher.ResourceType.EVENT_TYPE),
                        Mockito.eq(NakadiAuditLogPublisher.ActionType.UPDATED),
                        Mockito.eq(eventType.getName())
                );

        final Optional<EventType> oldEventType = oldEventCaptor.getValue().map(value -> (EventType) value);
        final Optional<EventType> newEventType = newEventCaptor.getValue().map(value -> (EventType) value);

        assertEquals(
                currentPartitions,
                requireNonNull(oldEventType.get().getDefaultStatistic()).getReadParallelism()
        );
        assertEquals(
                currentPartitions,
                requireNonNull(oldEventType.get().getDefaultStatistic()).getWriteParallelism()
        );
        assertEquals(
                desiredPartitions,
                requireNonNull(newEventType.get().getDefaultStatistic()).getReadParallelism()
        );
        assertEquals(
                desiredPartitions,
                requireNonNull(newEventType.get().getDefaultStatistic()).getWriteParallelism()
        );
    }
}