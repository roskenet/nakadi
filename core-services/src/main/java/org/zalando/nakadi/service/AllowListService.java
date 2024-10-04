package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedCountStrategy;
import org.echocat.jomon.runtime.concurrent.Retryer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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
    private String nodeId;

    @Autowired
    public AllowListService(final ZooKeeperHolder zooKeeperHolder,
                            final NakadiAuditLogPublisher auditLogPublisher,
                            final ObjectMapper objectMapper) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.auditLogPublisher = auditLogPublisher;
        this.objectMapper = objectMapper;
        this.nodeId = UUID.randomUUID().toString();
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

    public boolean canConnect(final Client client) {
        for (final Map.Entry<String, ChildData> entry :
                allowListCache.getCurrentChildren(PATH_ALLOWLIST).entrySet()) {
            if (entry.getKey().equals(client.getClientId())) {
                try {
                    final byte[] data = zooKeeperHolder.get().getData().forPath(PATH_CONNECTIONS + "/" + nodeId);
                    final AllowListData allowListData = objectMapper.readValue(data, AllowListData.class);
                    if (allowListData.getCurrentOpenConnections() > CONNS_PER_APPLICATION) {
                        return false;
                    } else {
                        break;
                    }
                } catch (final Exception e) {
                    LOG.error(e.getMessage(), e);
                    throw new NakadiRuntimeException(e);
                }
            }
        }

        return true;
    }

    public void incConnectionsCount() {
        updateConnectionsCount(1);
    }

    public void decConnectionsCount() {
        updateConnectionsCount(-1);
    }

    private void updateConnectionsCount(final Integer increment) {
        Retryer.executeWithRetry(() -> {
                    try {
                        final CuratorFramework curator = zooKeeperHolder.get();
                        final Stat stat = new Stat();
                        final byte[] data = curator.getData()
                                .storingStatIn(stat).forPath(PATH_CONNECTIONS + "/" + nodeId);
                        final AllowListData prevAllowListData = objectMapper.readValue(data, AllowListData.class);
                        final AllowListData nextAllowListData =
                                new AllowListData(prevAllowListData.getCurrentOpenConnections() + increment);

                        curator.setData().withVersion(stat.getVersion()).forPath(PATH_CONNECTIONS + "/" + nodeId,
                                objectMapper.writeValueAsBytes(nextAllowListData));
                    } catch (final Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                },
                new RetryForSpecifiedCountStrategy()
                        .withMaxNumberOfRetries(5)
                        .withExceptionsThatForceRetry(KeeperException.BadVersionException.class));
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

    private class AllowListData {
        private final Integer currentOpenConnections;

        AllowListData(final Integer currentOpenConnections) {
            this.currentOpenConnections = currentOpenConnections;
        }

        public Integer getCurrentOpenConnections() {
            return currentOpenConnections;
        }
    }
}
