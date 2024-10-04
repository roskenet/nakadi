package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Optional;

@Service
public class AllowListService {

    private static final Logger LOG = LoggerFactory.getLogger(AllowListService.class);
    private static final String PATH_ALLOWLIST = "/nakadi/lola/allowlist/applications";

    private final ZooKeeperHolder zooKeeperHolder;
    private final NakadiAuditLogPublisher auditLogPublisher;
    private TreeCache allowListCache;
    private final ObjectMapper objectMapper;

    @Autowired
    public AllowListService(final ZooKeeperHolder zooKeeperHolder,
                            final NakadiAuditLogPublisher auditLogPublisher,
                            final ObjectMapper objectMapper) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.auditLogPublisher = auditLogPublisher;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void initIt() {
        try {
            this.allowListCache = TreeCache.newBuilder(
                    zooKeeperHolder.get(), PATH_ALLOWLIST).setCacheData(false).build();
            this.allowListCache.start();
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void cleanUp() {
        this.allowListCache.close();
    }

    public boolean isAllowed(final String application) {
        try {
            final ChildData currentData = allowListCache.getCurrentData(application);
            if (currentData != null) {
                final AllowListData allowListData = objectMapper
                        .readValue(currentData.getData(), AllowListData.class);
                allowListData.getMaxOpenConnections();
                return true;
            }
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return false;
    }

    public void add(final String application) throws RuntimeException {
        try {
            final AllowListData allowListData = new AllowListData(100);
            final CuratorFramework curator = zooKeeperHolder.get();
            curator.create().creatingParentsIfNeeded().forPath(
                    PATH_ALLOWLIST + "/" + application,
                    objectMapper.writeValueAsBytes(allowListData)
            );

            auditLogPublisher.publish(
                    Optional.empty(),
                    Optional.of(allowListData),
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
                curator.delete().forPath(PATH_ALLOWLIST + "/" + application);

                // danger to lose the audit event fixme
                final AllowListData allowListData = objectMapper
                        .readValue(currentData.getData(), AllowListData.class);
                auditLogPublisher.publish(
                        Optional.of(allowListData),
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
        private final Integer maxOpenConnections;

        public AllowListData(final Integer maxOpenConnections) {
            this.maxOpenConnections = maxOpenConnections;
        }

        public Integer getMaxOpenConnections() {
            return maxOpenConnections;
        }
    }
}
