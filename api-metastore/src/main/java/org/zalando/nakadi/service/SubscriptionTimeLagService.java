package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.exceptions.runtime.ErrorGettingCursorTimeLagException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.service.timeline.HighLevelConsumer;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class SubscriptionTimeLagService {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionTimeLagService.class);
    private static final int EVENT_FETCH_WAIT_TIME_MS = 1000;
    private static final int REQUEST_TIMEOUT_MS = 30000;
    private static final int MAX_THREADS_PER_REQUEST = 20;
    private static final int TIME_LAG_COMMON_POOL_SIZE = 400;

    private final TimelineService timelineService;
    private final NakadiCursorComparator cursorComparator;
    private final ThreadPoolExecutor threadPool;

    @Autowired
    public SubscriptionTimeLagService(final TimelineService timelineService,
                                      final NakadiCursorComparator cursorComparator) {
        this.timelineService = timelineService;
        this.cursorComparator = cursorComparator;
        this.threadPool = new ThreadPoolExecutor(0, TIME_LAG_COMMON_POOL_SIZE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<>());
    }

    public Map<EventTypePartition, Duration> getTimeLags(
            final String subscriptionId,
            final Collection<NakadiCursor> committedPositions,
            final List<PartitionStatistics> stats) {

        final TimeLagRequestHandler timeLagHandler = new TimeLagRequestHandler(timelineService, threadPool);
        final Map<EventTypePartition, Duration> timeLags = new HashMap<>();
        final Map<EventTypePartition, CompletableFuture<Duration>> futureTimeLags = new HashMap<>();
        final Map<EventTypePartition, PartitionStatistics> statsByEventTypePartition =
                stats.stream()
                .collect(Collectors.toMap(
                                s -> s.getLast().getEventTypePartition(),
                                Function.identity()));
        try {
            for (final NakadiCursor cursor : committedPositions) {
                final EventTypePartition etp = cursor.getEventTypePartition();
                final Optional<PartitionStatistics> ps = Optional.ofNullable(statsByEventTypePartition.get(etp));
                final NakadiCursor cursorToReadFrom;

                // first, check if committed cursor is past data available in kafka and adjust accordingly
                final Optional<NakadiCursor> beforeFirst = ps.map(PartitionStatistics::getBeforeFirst);
                if (beforeFirst.isPresent() && cursorComparator.compare(beforeFirst.get(), cursor) > 0) {
                    cursorToReadFrom = beforeFirst.get();
                    LOG.debug("updated cursor to read from: {} (committed is {})", cursorToReadFrom, cursor);
                } else {
                    cursorToReadFrom = cursor;
                }

                // now check if committed cursor is in the tail position: then there's nothing to read and the lag is 0
                final Optional<NakadiCursor> last = ps.map(PartitionStatistics::getLast);
                if (last.isPresent() && cursorComparator.compare(cursorToReadFrom, last.get()) >= 0) {
                    timeLags.put(etp, Duration.ZERO);
                } else {
                    // OK, let's try to actually read some events
                    final CompletableFuture<Duration> timeLagFuture =
                            timeLagHandler.getCursorTimeLagFuture(cursorToReadFrom, last);
                    futureTimeLags.put(etp, timeLagFuture);
                }
            }
            CompletableFuture
                    .allOf(futureTimeLags.values().toArray(new CompletableFuture[futureTimeLags.size()]))
                    .get(timeLagHandler.getRemainingTimeoutMs(), TimeUnit.MILLISECONDS);

        } catch (final Exception e) {
            LOG.error("caught exception the timelag stats are not complete for subscription {}", subscriptionId, e);
        }

        for (final EventTypePartition etp : futureTimeLags.keySet()) {
            final CompletableFuture<Duration> future = futureTimeLags.get(etp);
            if (future.isDone() && !future.isCompletedExceptionally()) {
                final Duration lag = future.getNow(null);
                if (lag != null) {
                    timeLags.put(etp, lag);
                }
            }
        }
        return timeLags;
    }

    private static class TimeLagRequestHandler {

        private final TimelineService timelineService;
        private final ThreadPoolExecutor threadPool;
        private final Semaphore semaphore;
        private final long timeoutTimestampMs;

        TimeLagRequestHandler(final TimelineService timelineService, final ThreadPoolExecutor threadPool) {
            this.timelineService = timelineService;
            this.threadPool = threadPool;
            this.semaphore = new Semaphore(MAX_THREADS_PER_REQUEST);
            this.timeoutTimestampMs = System.currentTimeMillis() + REQUEST_TIMEOUT_MS;
        }

        CompletableFuture<Duration> getCursorTimeLagFuture(
                final NakadiCursor cursor, final Optional<NakadiCursor> lastCursor)
                throws InterruptedException, TimeoutException {

            final CompletableFuture<Duration> future = new CompletableFuture<>();
            if (semaphore.tryAcquire(getRemainingTimeoutMs(), TimeUnit.MILLISECONDS)) {
                threadPool.submit(() -> {
                    try {
                        final Duration timeLag = getNextEventTimeLag(cursor, lastCursor);
                        future.complete(timeLag);
                    } catch (final Exception e) {
                        future.completeExceptionally(e);
                    } finally {
                        semaphore.release();
                    }
                });
            } else {
                throw new TimeoutException("Partition time lag timeout exceeded");
            }
            return future;
        }

        long getRemainingTimeoutMs() {
            if (timeoutTimestampMs > System.currentTimeMillis()) {
                return timeoutTimestampMs - System.currentTimeMillis();
            } else {
                return 0;
            }
        }

        private Duration getNextEventTimeLag(final NakadiCursor cursor, final Optional<NakadiCursor> lastCursor)
                throws ErrorGettingCursorTimeLagException, InconsistentStateException {

            final String clientId = String.format("time-lag-checker-%s-%s",
                    cursor.getEventType(), cursor.getPartition());

            try (HighLevelConsumer consumer = timelineService.createEventConsumer(clientId)) {
                consumer.reassign(ImmutableList.of(cursor));

                final List<ConsumedEvent> events = consumer.readEvents();
                if (events.isEmpty()) {
                    // expected some events due to newer offsets being availabile, but got none: must be only tombstones
                    return Duration.ZERO;
                } else {
                    return Duration.ofMillis(new Date().getTime() - events.iterator().next().getTimestamp());
                }
            } catch (final Exception e) {
                throw new ErrorGettingCursorTimeLagException(cursor, lastCursor, e);
            }
        }
    }

}
