package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The class holds two responsibilities
 *
 * - allow list for lola consumers: list of allowed application is stored in zookeeper
 * - connection limit for lola consumers: we use approximation to calculate number of simultaneous connections
 * by multiplying current connections to the number of lola pods, the pods are registered in zookeeper as ephemeral nodes
 *
 */
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
    private final Map<String, Integer> clientConnections;
    private final FeatureToggleService featureToggleService;
    private final NakadiSettings nakadiSettings;

    @Autowired
    public AllowListService(final ZooKeeperHolder zooKeeperHolder,
                            final NakadiAuditLogPublisher auditLogPublisher,
                            final ObjectMapper objectMapper,
                            final FeatureToggleService featureToggleService,
                            final NakadiSettings nakadiSettings) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.auditLogPublisher = auditLogPublisher;
        this.objectMapper = objectMapper;
        this.nodeId = UUID.randomUUID().toString();
        this.clientConnections = new ConcurrentHashMap<>();
        this.featureToggleService = featureToggleService;
        this.nakadiSettings = nakadiSettings;
    }

    @PostConstruct
    public void initIt() {
        try {
            this.allowListCache = TreeCache.newBuilder(
                    zooKeeperHolder.get(), PATH_ALLOWLIST).setCacheData(false).build();
            this.allowListCache.start();

            if (nakadiSettings.isLimitLoLaConnections()) {
                zooKeeperHolder.get().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                        .forPath(PATH_CONNECTIONS + "/" + nodeId);
            }
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
            throw new NakadiRuntimeException(e);
        }

        return true;
    }

    public boolean canAcceptConnection(final Client client) {
        if (!featureToggleService.isFeatureEnabled(Feature.RATE_LIMIT_LOLA_CONNECTIONS)) {
            return true;
        }

        try {
            final CuratorFramework curator = zooKeeperHolder.get();
            final List<String> nodes = curator.getChildren().forPath(PATH_CONNECTIONS);
            final Integer currentConns = clientConnections.getOrDefault(client.getClientId(), 0);
            final Integer currentApproxConnects = nodes.size() * currentConns;
            if (currentApproxConnects > CONNS_PER_APPLICATION) {
                LOG.debug("Can not accept connection for client `{}`, connections: {}, nodes: {}, limit: {}",
                        client.getClientId(), currentConns, nodes, CONNS_PER_APPLICATION);
                return false;
            }

            return true;
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    public void trackConnectionsCount(final Client client, final Integer increment) {
        clientConnections.compute(client.getClientId(),
                (app, conns) -> Math.max(0, (conns == null ? 0 : conns) + increment));
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
