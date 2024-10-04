package org.zalando.nakadi.domain;

import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.util.Objects;
import java.util.Optional;

/**
 * Kafka record header to mark test events.
 * A missing header is interpreted as non-test event.
 */
public class TestProjectIdHeader {

    public static final String HEADER_NAME = "TEST_PROJECT_ID";
    private final String value;

    public TestProjectIdHeader(final String value) {
        this.value = value;
    }

    public void serialize(final ProducerRecord<byte[], byte[]> record) {
        record.headers().add(HEADER_NAME, value.getBytes(Charsets.UTF_8));
    }

    public static Optional<TestProjectIdHeader> deserialize(final ConsumerRecord<byte[], byte[]> record) {
        final Header matchedHeader = record.headers().lastHeader(TestProjectIdHeader.HEADER_NAME);
        if (null == matchedHeader) {
            return Optional.empty();
        }
        return Optional.of(new TestProjectIdHeader(
                new String(matchedHeader.value(), Charsets.UTF_8)
        ));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TestProjectIdHeader)) {
            return false;
        }
        final TestProjectIdHeader that = (TestProjectIdHeader) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
