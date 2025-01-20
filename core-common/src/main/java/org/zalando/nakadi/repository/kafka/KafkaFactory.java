package org.zalando.nakadi.repository.kafka;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFactory.class);
    private final KafkaLocationManager kafkaLocationManager;

    private final List<Producer<byte[], byte[]>> producers;

    private final BlockingQueue<KafkaConsumerProxy> consumerPool;
    private final Meter consumerCreateMeter;
    private final Meter consumerPoolTakeMeter;
    private final Meter consumerPoolReturnMeter;
    private final Timer consumerPoolWaitTimer;
    private final Timer consumerPoolHoldTimer;

    public KafkaFactory(final KafkaLocationManager kafkaLocationManager,
                        final MetricRegistry metricsRegistry,
                        final String storageId,
                        final int numActiveProducers,
                        final int consumerPoolSize) {
        this.kafkaLocationManager = kafkaLocationManager;

        LOG.info("Allocating {} Kafka producers for storage {}", numActiveProducers, storageId);
        this.producers = new ArrayList<>(numActiveProducers);
        for (int i = 0; i < numActiveProducers; ++i) {
            final String clientId = String.format("nakadi_%s-%d", storageId, i);
            this.producers.add(createProducerInstance(clientId));
        }

        if (consumerPoolSize > 0) {
            LOG.info("Preparing a pool of {} Kafka consumers for storage {}",
                    consumerPoolSize, storageId);
            this.consumerPool = new LinkedBlockingQueue(consumerPoolSize);
            for (int i = 0; i < consumerPoolSize; ++i) {
                this.consumerPool.add(createConsumerProxyInstance());
            }
        } else {
            this.consumerPool = null;
        }

        this.consumerCreateMeter = metricsRegistry.meter("nakadi.kafka.consumer.created");
        this.consumerPoolTakeMeter = metricsRegistry.meter("nakadi.kafka.consumer.taken");
        this.consumerPoolReturnMeter = metricsRegistry.meter("nakadi.kafka.consumer.returned");
        this.consumerPoolWaitTimer = metricsRegistry.timer("nakadi.kafka.consumer.wait.time");
        this.consumerPoolHoldTimer = metricsRegistry.timer("nakadi.kafka.consumer.hold.time");
    }

    public Producer<byte[], byte[]> takeProducer(final String topic) {
        final int index = Math.abs(topic.hashCode() % producers.size());
        return producers.get(index);
    }

    public Consumer<byte[], byte[]> getConsumer(final String clientId /* ignored */) {
        return getConsumer();
    }

    /**
     * If a pool of consumers is configured then a consumer is taken from the pool, otherwise a new consumer is created
     * and returned.
     *
     * The caller *has* to close the consumer once done working with it.  In case of a pooled consumer, "closing" will
     * result in it being returned to the pool for later re-use.
     */
    public Consumer<byte[], byte[]> getConsumer() {
        if (consumerPool != null) {
            return takeConsumer();
        }

        final Consumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(kafkaLocationManager.getKafkaConsumerProperties());

        consumerCreateMeter.mark();

        return consumer;
    }

    private Consumer<byte[], byte[]> takeConsumer() {
        final KafkaConsumerProxy consumer;

        LOG.trace("Taking a consumer from the pool");
        try (Timer.Context unused = consumerPoolWaitTimer.time()) {
            // TODO: expose the timeout as a parameter
            consumer = consumerPool.poll(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while waiting for a consumer from the pool");
        }
        if (consumer == null) {
            throw new RuntimeException("timed out while waiting for a consumer from the pool");
        }

        consumerPoolTakeMeter.mark();
        consumer.markTaken();

        return consumer;
    }

    private void returnConsumer(final KafkaConsumerProxy consumer) {
        LOG.trace("Returning a consumer to the pool");

        consumer.assign(Collections.emptyList());

        consumerPoolHoldTimer.update(
                System.currentTimeMillis() - consumer.getTakenSystemTimeMillis(), TimeUnit.MILLISECONDS);
        consumerPoolReturnMeter.mark();

        try {
            consumerPool.put(consumer);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while putting a consumer back to the pool");
        }
    }

    protected Producer<byte[], byte[]> createProducerInstance(@Nullable final String clientId) {
        return new KafkaProducer<>(kafkaLocationManager.getKafkaProducerProperties(
                Optional.ofNullable(clientId)));
    }

    protected KafkaConsumerProxy createConsumerProxyInstance() {
        return new KafkaConsumerProxy(kafkaLocationManager.getKafkaConsumerProperties());
    }

    public class KafkaConsumerProxy extends KafkaConsumer<byte[], byte[]> {

        private long takenSystemTimeMillis = -1;

        KafkaConsumerProxy(final Properties properties) {
            super(properties);
        }

        void markTaken() {
            takenSystemTimeMillis = System.currentTimeMillis();
        }

        long getTakenSystemTimeMillis() {
            return takenSystemTimeMillis;
        }

        @Override
        public void close() {
            returnConsumer(this);
        }
    }
}
