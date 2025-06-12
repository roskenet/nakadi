package org.zalando.nakadi.domain;

import org.json.JSONObject;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.authz.ResourceType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class ConsumedEvent implements Resource<ConsumedEvent> {

    private final byte[] key;
    private final byte[] event;
    private final NakadiCursor position;
    private final long timestamp;
    private final EventOwnerHeader owner;
    private final Map<HeaderTag, String> consumerTags;
    private final Optional<TestProjectIdHeader> testProjectIdHeader;
    private byte[] tombstoneEvent;

    public ConsumedEvent(final byte[] key, final byte[] event,
                         final NakadiCursor position, final long timestamp,
                         @Nullable final EventOwnerHeader owner, final Map<HeaderTag, String> consumerTags,
                         final Optional<TestProjectIdHeader> testProjectIdHeader) {
        this.key = key;
        this.event = event;
        this.position = position;
        this.timestamp = timestamp;
        this.owner = owner;
        this.consumerTags = consumerTags;
        this.testProjectIdHeader = testProjectIdHeader;
        this.tombstoneEvent = createTombstonePayload();
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getEvent() {
        return event;
    }

    public byte[] getPayload() {
        if (isTombstone()) {
           return tombstoneEvent;
        }
        return event;
    }

   public NakadiCursor getPosition() {
        return position;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Optional<TestProjectIdHeader> getTestProjectIdHeader() {
        return testProjectIdHeader;
    }

    public boolean isTombstone() {
        return event == null;
    }

    private byte[] createTombstonePayload() {
        if (!isTombstone()) {
            return null;
        }
        final JSONObject metadata = new JSONObject();
        metadata
                .put("event_type", getConsumerTags()
                        .get(HeaderTag.PUBLISHED_EVENT_TYPE))
                // the partition might be wrong due to misplaced events bug, but filtering based on et name
                // is done before the event is sent to user
                .put("partition", getPosition().getPartition())
                .put("partition_compaction_key", new String(getKey(), StandardCharsets.UTF_8))
                .put("is_tombstone", true);

        getTestProjectIdHeader().ifPresent(
                testProjectIdHeader -> metadata.put("test_project_id", testProjectIdHeader.getValue()));
        return new JSONObject()
                .put("metadata", metadata)
                .toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConsumedEvent)) {
            return false;
        }

        final ConsumedEvent that = (ConsumedEvent) o;
        // TODO: compare array contents?
        return Objects.equals(this.event, that.event)
                && Objects.equals(this.tombstoneEvent, that.tombstoneEvent)
                && Objects.equals(this.position, that.position);
    }

    @Override
    public int hashCode() {
        return position.hashCode();
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getType() {
        return ResourceType.EVENT_RESOURCE;
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        if (operation == AuthorizationService.Operation.READ) {
            if (null == this.owner) {
                return Optional.of(Collections.emptyList());
            } else {
                return Optional.of(this.owner)
                        .map(ConsumedEvent::authToAttribute)
                        .map(Collections::singletonList);
            }
        }
        // The only supported operation for Consumed event is READ.
        return Optional.empty();
    }

    @Override
    public ConsumedEvent get() {
        return this;
    }

    @Override
    public Map<String, List<AuthorizationAttribute>> getAuthorization() {
        return Collections.emptyMap();
    }

    public static AuthorizationAttribute authToAttribute(final EventOwnerHeader auth) {
        return new AuthorizationAttributeProxy(auth);
    }

    public Map<HeaderTag, String> getConsumerTags() {
        return consumerTags;
    }
}
