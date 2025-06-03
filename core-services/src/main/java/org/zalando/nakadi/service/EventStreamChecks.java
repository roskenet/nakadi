package org.zalando.nakadi.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.cache.SubscriptionCache;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.HeaderTag;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.kafka.KafkaRecordDeserializer;

import java.io.IOException;
import java.util.Collection;

/**
 * EventStreamChecks is responsible for validating event consumption permissions and data quality.
 * It serves as a gatekeeper for event streaming operations in Nakadi, ensuring that:
 *
 * 1. Events are only delivered to authorized consumers (via blacklisting and authorization checks)
 * 2. Events meet quality criteria (e.g., are not misplaced in wrong event types)
 *
 * This service is used throughout the system in various streaming contexts:
 * - Low-Level API
 * - Subscriptions API
 * - Cursor operations
 */
@Service
public class EventStreamChecks {

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamChecks.class);

    private final BlacklistService blacklistService;
    private final AuthorizationService authorizationService;
    private final SubscriptionCache subscriptionCache;
    private final EventTypeCache eventTypeCache;
    private final KafkaRecordDeserializer kafkaRecordDeserializer;
    private final FeatureToggleService featureToggleService;

    public EventStreamChecks(
            final BlacklistService blacklistService,
            final AuthorizationService authorizationService,
            final KafkaRecordDeserializer kafkaRecordDeserializer,
            final SubscriptionCache subscriptionCache,
            final FeatureToggleService featureToggleService,
            final EventTypeCache eventTypeCache) {
        this.blacklistService = blacklistService;
        this.authorizationService = authorizationService;
        this.subscriptionCache = subscriptionCache;
        this.eventTypeCache = eventTypeCache;
        this.featureToggleService = featureToggleService;
        this.kafkaRecordDeserializer = kafkaRecordDeserializer;
    }

    /**
     * Checks if consumption is blocked for a specific application or any of the provided event types.
     * 
     * @param etNames Collection of event type names to check
     * @param appId Application ID to check
     * @return true if consumption is blocked, false otherwise
     */
    public boolean isConsumptionBlocked(final Collection<String> etNames, final String appId) {
        return blacklistService.isConsumptionBlocked(etNames, appId);
    }

    /**
     * Performs fine-grained authorization check at the individual event level.
     * 
     * @param evt The event to check authorization for
     * @return true if consumption is blocked (not authorized), false otherwise
     */
    public boolean isConsumptionBlocked(final ConsumedEvent evt) {
        return !authorizationService.isAuthorized(AuthorizationService.Operation.READ, evt);
    }

    /**
     * Checks if consumption is blocked for a specific subscription.
     * This method retrieves the event types associated with the subscription
     * and checks if any of them or the application are blacklisted.
     * 
     * @param subscriptionId ID of the subscription to check
     * @param appId Application ID to check
     * @return true if consumption is blocked, false otherwise
     */
    public boolean isSubscriptionConsumptionBlocked(final String subscriptionId, final String appId) {
        try {
            return blacklistService.isConsumptionBlocked(
                    subscriptionCache.getSubscription(subscriptionId).getEventTypes(), appId);
        } catch (final NoSuchSubscriptionException e) {
            // It's fine, subscription doesn't exists.
        } catch (final ServiceTemporarilyUnavailableException e) {
            LOG.error(e.getMessage(), e);
        }
        return false;
    }

    /**
     * Detects events that are published to the wrong event type.
     * This method compares the actual event type name from the event metadata
     * with the expected event type name from the event's position.
     * Only applies to event types with category other than UNDEFINED.
     * 
     * @param event The event to check for misplacement
     * @return true if the event is misplaced and feature flag is enabled, false otherwise
     * @throws NakadiRuntimeException if metadata parsing fails
     */
    public boolean shouldSkipMisplacedEvent(final ConsumedEvent event) {
        if (featureToggleService.isFeatureEnabled(Feature.SKIP_MISPLACED_EVENTS)) {
           final String expectedEventTypeName = event.getPosition().getEventType();
            if (eventTypeCache.getEventType(expectedEventTypeName).getCategory() != EventCategory.UNDEFINED) {
                try {
                    final String actualEventTypeName = kafkaRecordDeserializer.getEventTypeName(event.getPayload());

                    if (!expectedEventTypeName.equals(actualEventTypeName)) {
                        LOG.warn("Consumed event for event type '{}', but expected '{}' (at position {}), topic id: {}",
                                actualEventTypeName, expectedEventTypeName, event.getPosition(),
                                event.getConsumerTags().get(HeaderTag.DEBUG_PUBLISHER_TOPIC_ID));
                        return true;
                    }
                } catch (final IOException e) {
                    throw new NakadiRuntimeException(
                            String.format("Failed to parse metadata to check for misplaced" +
                                            " event in '%s' at position %s",
                                    expectedEventTypeName, event.getPosition()),
                            e);
                }
            }
            return false;
        }
        return false;
    }
}
