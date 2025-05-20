package org.zalando.nakadi.metrics;

import com.codahale.metrics.MetricRegistry;

public class MetricUtils {

    // PREFIXES
    public static final String NAKADI_PREFIX = "nakadi.";
    public static final String EVENTTYPES_PREFIX = NAKADI_PREFIX + "eventtypes";
    public static final String SUBSCRIPTION_PREFIX = NAKADI_PREFIX + "subscriptions";
    private static final String LOLA_PREFIX = "lola";
    private static final String HILA_PREFIX = "hila";
    private static final String LOLA_STREAM_PREFIX = "lola-stream";
    private static final String HILA_STREAM_PREFIX = "hila-stream";

    // METRIC NAMES
    private static final String BYTES_FLUSHED = "bytes-flushed";
    private static final String OPEN_CONNECTIONS = "open-connections";
    public static final String SSF_EVENTS_TOTAL = "ssf-events-total";
    public static final String SSF_EVENTS_MATCHED = "ssf-events-matched";

    public static String metricNameFor(final String eventTypeName, final String metricName) {
        return MetricRegistry.name(EVENTTYPES_PREFIX, eventTypeName.replace('.', '#'), metricName);
    }

    public static String metricNameForSubscription(final String subscriptionId, final String metricName) {
        return MetricRegistry.name(SUBSCRIPTION_PREFIX, subscriptionId, metricName);
    }

    public static String metricNameForLoLAStream(final String applicationId, final String eventTypeName) {
        return MetricRegistry.name(
                LOLA_PREFIX,
                applicationId.replace(".", "#"),
                eventTypeName.replace(".", "#"),
                BYTES_FLUSHED);
    }

    public static String metricNameForLoLAOpenConnections(final String applicationId) {
        return MetricRegistry.name(
                LOLA_PREFIX,
                applicationId.replace(".", "#"),
                OPEN_CONNECTIONS);
    }

    public static String metricNameForHiLALink(final String applicationId, final String subscriptionId) {
        return MetricRegistry.name(
                HILA_PREFIX,
                applicationId.replace(".", "#"),
                subscriptionId,
                BYTES_FLUSHED);
    }

    public static String metricNameForLolaStream(
            final String applicationId, // TODO users?
            final String eventTypeName,
            final String streamId,
            final String metricName) {
        return MetricRegistry.name(
                LOLA_STREAM_PREFIX,
                applicationId.replace(".", "#"),
                eventTypeName.replace(".", "#"),
                streamId,
                metricName);
    }

    public static String metricNameForHiLAStream(
            final String subscriptionId,
            final String streamId,
            final String metricName) {
        return MetricRegistry.name(
                HILA_STREAM_PREFIX,
                subscriptionId,
                streamId,
                metricName);
    }

}
