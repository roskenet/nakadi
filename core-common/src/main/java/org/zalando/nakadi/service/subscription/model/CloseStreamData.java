package org.zalando.nakadi.service.subscription.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;

public class CloseStreamData {

    private final Set<String> streamIdsToClose;
    // TODO: include the reason for close, to propagate to client streams

    @JsonCreator
    public CloseStreamData(@JsonProperty("stream_ids") final Set<String> streamIdsToClose) {
        this.streamIdsToClose = streamIdsToClose;
    }

    public Set<String> getStreamIdsToClose() {
        return streamIdsToClose;
    }

    @Override
    public String toString() {
        return "CloseStreamData{streamIdsToClose=" + streamIdsToClose + "}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CloseStreamData other = (CloseStreamData) o;
        return Objects.equals(streamIdsToClose, other.getStreamIdsToClose());
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamIdsToClose);
    }
}
