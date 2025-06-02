package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.service.timeline.HighLevelConsumer;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SubscriptionTimeLagServiceTest {

    private static final long FAKE_EVENT_TIMESTAMP = 478220400000L;

    private NakadiCursorComparator cursorComparator;
    private SubscriptionTimeLagService timeLagService;
    private TimelineService timelineService;

    @Before
    public void setUp() {
        timelineService = mock(TimelineService.class);

        cursorComparator = mock(NakadiCursorComparator.class);
        timeLagService = new SubscriptionTimeLagService(timelineService, cursorComparator);
    }

    @Test
    public void testTimeLagsForBeforeFirstAndTailAndNotTailPositions() throws InvalidCursorException {

        final HighLevelConsumer eventConsumer = mock(HighLevelConsumer.class);
        final Timeline timeline = mock(Timeline.class);
        when(timeline.getStorage()).thenReturn(new Storage("", Storage.Type.KAFKA));
        when(eventConsumer.readEvents())
                .thenAnswer(invocation ->
                        ImmutableList.of(
                                new ConsumedEvent(null,
                                        new byte[0], NakadiCursor.of(timeline, "", ""),
                                        FAKE_EVENT_TIMESTAMP, null, null, Optional.empty())));

        when(timelineService.createEventConsumer(any())).thenReturn(eventConsumer);

        final Timeline et1Timeline = new Timeline("et1", 0, new Storage("", Storage.Type.KAFKA), "t1", null);

        final NakadiCursor committedCursor0 = NakadiCursor.of(et1Timeline, "p0", "o0");
        final NakadiCursor committedCursor1 = NakadiCursor.of(et1Timeline, "p1", "o1");
        final NakadiCursor committedCursor2 = NakadiCursor.of(et1Timeline, "p2", "o2");

        final PartitionStatistics stats0 = mockStats(
                NakadiCursor.of(et1Timeline, "p0", "bf0"),
                NakadiCursor.of(et1Timeline, "p0", "f0"),
                NakadiCursor.of(et1Timeline, "p0", "l0"));

        final PartitionStatistics stats1 = mockStats(
                NakadiCursor.of(et1Timeline, "p1", "bf1"),
                NakadiCursor.of(et1Timeline, "p1", "f1"),
                NakadiCursor.of(et1Timeline, "p1", "l1"));

        final PartitionStatistics stats2 = mockStats(
                NakadiCursor.of(et1Timeline, "p2", "bf2"),
                NakadiCursor.of(et1Timeline, "p2", "f2"),
                NakadiCursor.of(et1Timeline, "p2", "l2"));

        // mock so that we already skipped some events
        when(cursorComparator.compare(stats0.getBeforeFirst(), committedCursor0)).thenReturn(10);

        // mock so that committed is between first and last
        when(cursorComparator.compare(stats1.getBeforeFirst(), committedCursor1)).thenReturn(-10);
        when(cursorComparator.compare(stats2.getBeforeFirst(), committedCursor2)).thenReturn(-20);

        // expect committed cursor to be shifted to the "before first" and report it's not the tail
        when(cursorComparator.compare(stats0.getBeforeFirst(), stats0.getLast())).thenReturn(-10);

        // mock first committed cursor to be at the tail - the expected time lag should be 0
        when(cursorComparator.compare(committedCursor1, stats1.getLast())).thenReturn(0);

        // mock second committed cursor to be lower than tail - the expected time lag should be > 0
        when(cursorComparator.compare(committedCursor2, stats2.getLast())).thenReturn(-1);

        final Map<EventTypePartition, Duration> timeLags = timeLagService.getTimeLags(
                "sub1",
                ImmutableList.of(committedCursor0, committedCursor1, committedCursor2),
                ImmutableList.of(stats0, stats1, stats2));

        assertThat(timeLags.entrySet(), hasSize(3));
        assertThat(timeLags.get(new EventTypePartition("et1", "p0")), greaterThan(Duration.ZERO));
        assertThat(timeLags.get(new EventTypePartition("et1", "p1")), equalTo(Duration.ZERO));
        assertThat(timeLags.get(new EventTypePartition("et1", "p2")), greaterThan(Duration.ZERO));
    }

    @Test
    public void whenExpectSomeEventsButAllAreTombstonesReportZeroTimeLag() throws InvalidCursorException {

        final HighLevelConsumer eventConsumer = mock(HighLevelConsumer.class);
        final Timeline timeline = mock(Timeline.class);
        when(timeline.getStorage()).thenReturn(new Storage("", Storage.Type.KAFKA));
        when(eventConsumer.readEvents())
                .thenAnswer(invocation -> ImmutableList.of());

        when(timelineService.createEventConsumer(any())).thenReturn(eventConsumer);

        final Timeline et1Timeline = new Timeline("et1", 0, new Storage("", Storage.Type.KAFKA), "t1", null);

        final NakadiCursor committedCursor1 = NakadiCursor.of(et1Timeline, "p1", "o1");

        final PartitionStatistics stats1 = mockStats(
                NakadiCursor.of(et1Timeline, "p1", "bf1"),
                NakadiCursor.of(et1Timeline, "p1", "f1"),
                NakadiCursor.of(et1Timeline, "p1", "l1"));

        // mock the committed cursor to be lower than tail - so we will try to read events to check time lag
        when(cursorComparator.compare(committedCursor1, stats1.getLast())).thenReturn(-1);

        final Map<EventTypePartition, Duration> timeLags = timeLagService.getTimeLags(
                "sub1",
                ImmutableList.of(committedCursor1),
                ImmutableList.of(stats1));

        assertThat(timeLags.entrySet(), hasSize(1));
        assertThat(timeLags.get(new EventTypePartition("et1", "p1")), equalTo(Duration.ZERO));
    }

    @Test
    public void whenNoSubscriptionThenReturnSizeZeroMap() {
        when(timelineService.createEventConsumer(any())).thenReturn(null);
        final Timeline et1Timeline = new Timeline("et1", 0, new Storage("", Storage.Type.KAFKA), "t1", null);
        final NakadiCursor committedCursor1 = NakadiCursor.of(et1Timeline, "p1", "o1");

        final Map<EventTypePartition, Duration> result = timeLagService.getTimeLags(
                "sub1",
                ImmutableList.of(committedCursor1),
                ImmutableList.of());
        assertThat(result.size(), is(0));
    }

    private PartitionStatistics mockStats(
            final NakadiCursor beforeFirst, final NakadiCursor first, final NakadiCursor last) {

        final PartitionStatistics stats = mock(PartitionStatistics.class);
        when(stats.getBeforeFirst()).thenReturn(beforeFirst);
        when(stats.getFirst()).thenReturn(first);
        when(stats.getLast()).thenReturn(last);
        return stats;
    }
}
