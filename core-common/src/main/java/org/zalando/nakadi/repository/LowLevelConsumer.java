package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.HeaderTag;
import org.zalando.nakadi.domain.TestProjectIdHeader;
import org.zalando.nakadi.domain.TopicPartition;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public interface LowLevelConsumer extends Closeable {

    Set<TopicPartition> getAssignment();

    List<Event> readEvents();

    class Event {
        private final byte[] data;
        private final String topic;
        private final int partition;
        private final long offset;
        private final long timestamp;
        private final EventOwnerHeader eventOwnerHeader;
        private final Map<HeaderTag, String> consumerTags;
        private final Optional<TestProjectIdHeader> testProjectIdHeader;

        public Event(final byte[] data,
                     final String topic,
                     final int partition,
                     final long offset,
                     final long timestamp,
                     final EventOwnerHeader eventOwnerHeader,
                     final Map<HeaderTag, String> consumerTags,
                     final Optional<TestProjectIdHeader> testProjectIdHeader) {
            this.data = data;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.timestamp = timestamp;
            this.eventOwnerHeader = eventOwnerHeader;
            this.consumerTags = consumerTags;
            this.testProjectIdHeader = testProjectIdHeader;
        }

        public byte[] getData() {
            return data;
        }

        public String getTopic() {
            return topic;
        }

        public int getPartition() {
            return partition;
        }

        public long getOffset() {
            return offset;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public EventOwnerHeader getEventOwnerHeader() {
            return eventOwnerHeader;
        }

        public Map<HeaderTag, String> getConsumerTags() {
            return consumerTags;
        }

        public Optional<TestProjectIdHeader> getTestProjectIdHeader() {
            return testProjectIdHeader;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Event)) {
                return false;
            }
            final Event event = (Event) o;
            return partition == event.partition
                    && offset == event.offset
                    && timestamp == event.timestamp
                    && Arrays.equals(data, event.data) && Objects.equals(topic, event.topic)
                    && Objects.equals(eventOwnerHeader, event.eventOwnerHeader)
                    && Objects.equals(consumerTags, event.consumerTags)
                    && Objects.equals(testProjectIdHeader, event.testProjectIdHeader);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(topic, partition, offset, timestamp, eventOwnerHeader,
                    consumerTags, testProjectIdHeader);
            result = 31 * result + Arrays.hashCode(data);
            return result;
        }
    }
}
