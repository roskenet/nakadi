package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class AllowListService {

    private static final Logger LOG = LoggerFactory.getLogger(AllowListService.class);
    private static final String PATH_ALLOWLIST = "/nakadi/lola/allowlist/applications";
    private static final String PATH_CONNECTIONS = "/nakadi/lola/connections";
    private static final int CONNS_PER_APPLICATION = 100;
    private final ZooKeeperHolder zooKeeperHolder;
    private final NakadiAuditLogPublisher auditLogPublisher;
    private TreeCache allowListCache;
    private final ObjectMapper objectMapper;
    private final String nodeId;
    private final Map<String, Integer> applicationConnections;
    private final FeatureToggleService featureToggleService;

    @Autowired
    public AllowListService(final ZooKeeperHolder zooKeeperHolder,
                            final NakadiAuditLogPublisher auditLogPublisher,
                            final ObjectMapper objectMapper,
                            final FeatureToggleService featureToggleService) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.auditLogPublisher = auditLogPublisher;
        this.objectMapper = objectMapper;
        this.nodeId = UUID.randomUUID().toString();
        this.applicationConnections = new ConcurrentHashMap<>();
        this.featureToggleService = featureToggleService;
    }

    @PostConstruct
    public void initIt() {
        try {
            this.allowListCache = TreeCache.newBuilder(
                    zooKeeperHolder.get(), PATH_ALLOWLIST).setCacheData(false).build();
            this.allowListCache.start();

            zooKeeperHolder.get().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                    .forPath(PATH_CONNECTIONS + "/" + nodeId);

        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void cleanUp() {
        this.allowListCache.close();
    }

    public boolean isAllowed(final Client client) {
        if (!featureToggleService.isFeatureEnabled(Feature.LOLA_CONNECTIONS_ALLOWLIST)) {
            return true;
        }

        if ("/employees".equals(client.getRealm())) {
            return true;
        }

        try {
            final ChildData currentData = allowListCache.getCurrentData(client.getClientId());
            if (currentData == null) {
                return false;
            }
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
            throw new NakadiRuntimeException(e);
        }

        return true;
    }

    public boolean canAcceptConnection(final Client client) {
        if (!featureToggleService.isFeatureEnabled(Feature.RATE_LIMIT_LOLA_CONNECTIONS)) {
            return true;
        }

        try {
            final Map<String, Integer> tmp = new HashMap<>(applicationConnections);
            final CuratorFramework curator = zooKeeperHolder.get();
            for (final String node : curator.getChildren().forPath(PATH_CONNECTIONS)) {
                final byte[] bytes = curator.getData().forPath(PATH_CONNECTIONS + "/" + node);
                final Map<String, Integer> appConns = objectMapper.readValue(bytes, HashMap.class);
                appConns.forEach((app1, conn1) ->
                        tmp.compute(app1, (app2, conn2) -> conn2 == null ? conn1 : conn1 + conn2));
            }

            final Integer currentConns = tmp.getOrDefault(client.getClientId(), 0);

            if (currentConns > CONNS_PER_APPLICATION) {
                return false;
            }

            return true;
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
            throw new NakadiRuntimeException(e);
        }
    }

    public void trackConnectionsCount(final Client client, final Integer increment) {
        applicationConnections.compute(client.getClientId(), (app, conns) -> conns == null ? 1 : 1 + increment);
    }

    @Scheduled(fixedRate = 5000)
    private void uploadApplicationConnections() {
        try {
            final CuratorFramework curator = zooKeeperHolder.get();
            curator.setData().forPath(PATH_CONNECTIONS + "/" + nodeId,
                    objectMapper.writeValueAsBytes(applicationConnections));
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public void add(final String application) throws RuntimeException {
        try {
            final CuratorFramework curator = zooKeeperHolder.get();
            curator.create().creatingParentsIfNeeded().forPath(
                    String.format("%s/%s", PATH_ALLOWLIST, application),
                    null
            );

            auditLogPublisher.publish(
                    Optional.empty(),
                    Optional.of(application),
                    NakadiAuditLogPublisher.ResourceType.BLACKLIST_ENTRY,
                    NakadiAuditLogPublisher.ActionType.CREATED,
                    application);
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while creating node in zk", e);
        }
    }

    public void remove(final String application) throws RuntimeException {
        try {
            final CuratorFramework curator = zooKeeperHolder.get();
            final ChildData currentData = allowListCache.getCurrentData(application);
            if (currentData != null) {
                curator.delete().forPath(
                        String.format("%s/%s", PATH_ALLOWLIST, application));

                // danger to lose the audit event fixme
                auditLogPublisher.publish(
                        Optional.of(application),
                        Optional.empty(),
                        NakadiAuditLogPublisher.ResourceType.BLACKLIST_ENTRY,
                        NakadiAuditLogPublisher.ActionType.DELETED,
                        application);
            }
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while deleting node from zk", e);
        }
    }
}
