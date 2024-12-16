package org.zalando.nakadi.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The class holds two responsibilities
 * <p>
 * - allow list for lola consumers:
 * list of allowed application is stored in zookeeper
 * <p>
 * - connection limit for lola consumers: we use approximation
 * to calculate number of simultaneous connections
 * by multiplying current connections to the number of lola pods,
 * the pods are registered in zookeeper as ephemeral nodes
 */
@Service
@Profile("!test")
public class AllowListService {

    private static final Logger LOG = LoggerFactory.getLogger(AllowListService.class);
    private static final String PATH_ALLOWLIST = "/nakadi/lola/allowlist/applications";
    private static final String PATH_NODES = "/nakadi/lola/nodes";
    private static final int CONNS_PER_APPLICATION = 100;
    private final ZooKeeperHolder zooKeeperHolder;
    private final NakadiAuditLogPublisher auditLogPublisher;
    private TreeCache allowListCache;
    private TreeCache nodesCache;
    private final String nodeId;
    private final Map<String, Integer> clientConnections;
    private final FeatureToggleService featureToggleService;
    private final NakadiSettings nakadiSettings;

    @Autowired
    public AllowListService(final ZooKeeperHolder zooKeeperHolder,
                            final NakadiAuditLogPublisher auditLogPublisher,
                            final FeatureToggleService featureToggleService,
                            final NakadiSettings nakadiSettings) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.auditLogPublisher = auditLogPublisher;
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
                this.nodesCache = TreeCache.newBuilder(
                        zooKeeperHolder.get(), PATH_NODES).setCacheData(false).build();
                this.nodesCache.start();

                this.zooKeeperHolder.get().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                        .forPath(PATH_NODES + "/" + nodeId);
            }
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @PreDestroy
    public void cleanUp() {
        this.allowListCache.close();
        if (nodesCache != null) {
            this.nodesCache.close();
        }
    }

    public boolean isAllowed(final Client client) {
        if (!featureToggleService.isFeatureEnabled(Feature.LOLA_CONNECTIONS_ALLOWLIST)) {
            return true;
        }

        if ("/employees".equals(client.getRealm())) {
            return true;
        }

        try {
            final ChildData currentData = allowListCache.getCurrentData(
                    PATH_ALLOWLIST + "/" + client.getClientId());
            if (currentData == null) {
                return false;
            }
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }

        return true;
    }

    public boolean canAcceptConnection(final Client client) {
        if (!featureToggleService.isFeatureEnabled(Feature.LIMIT_LOLA_CONNECTIONS)) {
            return true;
        }

        try {
            // -1, do not count root (PATH_NODES) itself, only child nodes
            // count at least one node, as we know this pod is definitely alive
            final Integer nodes = Math.max(1, nodesCache.size() - 1);
            final Integer currentConns = clientConnections.getOrDefault(client.getClientId(), 0);
            final Integer currentApproxConnects = nodes * currentConns;

            LOG.debug("Client: {}, connections: {}, nodes: {}, limit: {}",
                    client.getClientId(), currentConns, nodes, CONNS_PER_APPLICATION);

            if (currentApproxConnects > CONNS_PER_APPLICATION) {
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
        } catch (final KeeperException.NodeExistsException e) {
            // skipping the node is already in place
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while creating node in zk", e);
        }
    }

    public void remove(final String application) throws RuntimeException {
        try {
            final CuratorFramework curator = zooKeeperHolder.get();
            final String applicationPath = String.format("%s/%s", PATH_ALLOWLIST, application);
            final ChildData currentData = allowListCache.getCurrentData(applicationPath);
            if (currentData != null) {
                curator.delete().forPath(applicationPath);

                // danger to lose the audit event fixme
                auditLogPublisher.publish(
                        Optional.of(application),
                        Optional.empty(),
                        NakadiAuditLogPublisher.ResourceType.BLACKLIST_ENTRY,
                        NakadiAuditLogPublisher.ActionType.DELETED,
                        application);
            }
        } catch (final KeeperException.NoNodeException e) {
            // skipping the node was deleted or did not exist
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while deleting node from zk", e);
        }
    }

    public Set<String> list() throws RuntimeException {
        try {
            final Map<String, ChildData> appsData = allowListCache
                    .getCurrentChildren(PATH_ALLOWLIST);
            if (appsData == null) {
                return Collections.emptySet();
            }
            return appsData.keySet();
        } catch (final Exception e) {
            throw new RuntimeException("Issue occurred while deleting node from zk", e);
        }
    }
}
