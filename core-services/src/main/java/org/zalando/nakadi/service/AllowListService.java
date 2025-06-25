package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private static final int DEFAULT_MAX_TOTAL_CONNECTIONS = 100;
    private final ZooKeeperHolder zooKeeperHolder;
    private final NakadiAuditLogPublisher auditLogPublisher;
    private TreeCache allowListCache;
    private TreeCache nodesCache;
    private PersistentNode persistentNode;
    private final String nodeId;
    private final Map<String, Integer> clientConnections;
    private final FeatureToggleService featureToggleService;
    private final NakadiSettings nakadiSettings;
    private final ObjectMapper objectMapper;

    @Autowired
    public AllowListService(final ZooKeeperHolder zooKeeperHolder,
                            final NakadiAuditLogPublisher auditLogPublisher,
                            final FeatureToggleService featureToggleService,
                            final NakadiSettings nakadiSettings,
                            @Value("${hostname:}") final String nodeId,
                            final ObjectMapper objectMapper) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.auditLogPublisher = auditLogPublisher;
        this.clientConnections = new ConcurrentHashMap<>();
        this.featureToggleService = featureToggleService;
        this.nakadiSettings = nakadiSettings;
        this.nodeId = nodeId;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void initIt() {
        try {
            this.allowListCache = TreeCache.newBuilder(zooKeeperHolder.get(), PATH_ALLOWLIST).build();
            this.allowListCache.start();

            if (nakadiSettings.isLimitLoLaConnections()) {
                this.nodesCache =
                        TreeCache
                                .newBuilder(zooKeeperHolder.get(), PATH_NODES)
                                .setCacheData(false)
                                .build();
                this.nodesCache.start();

                final String nodePath = PATH_NODES + "/" + nodeId;
                this.persistentNode = new PersistentNode(
                        this.zooKeeperHolder.get(),
                        CreateMode.EPHEMERAL,
                        false,
                        nodePath,
                        new byte[0],
                        true
                );
                this.persistentNode.start();
            }
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    @PreDestroy
    public void cleanUp() throws IOException {
        this.allowListCache.close();
        if (persistentNode != null) {
            persistentNode.close();
        }
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
            final String application = client.getClientId();
            if (allowListCache.getCurrentData(makeApplicationPath(application)) == null) {
                return false;
            }
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }

        return true;
    }

    public boolean canAcceptConnection(final Client client) {
        if (!nakadiSettings.isLimitLoLaConnections()) {
            // this node is not configured to limit, so allow any new connections
            return true;
        }
        if (!featureToggleService.isFeatureEnabled(Feature.LIMIT_LOLA_CONNECTIONS)) {
            // the node is configured to limit, but currently the feature toggle is unset by the operator
            return true;
        }
        try {
            final String application = client.getClientId();
            //
            // 1. -1, do not count root (PATH_NODES) itself, only child nodes
            // 2. count at least one node, as we know this pod is definitely alive
            //
            final int nodes = Math.max(1, nodesCache.size() - 1);
            final int thisNodeConnections = clientConnections.getOrDefault(application, 0);
            final int approxTotalConnections = nodes * thisNodeConnections;

            final int maxTotalConnections = getCachedApplicationConfig(application)
                    .map(ApplicationConfig::getMaxTotalConnections)
                    .orElse(DEFAULT_MAX_TOTAL_CONNECTIONS);

            LOG.debug("Application: {}, this node conns: {}, nodes: {}, approx. total conns: {}, max total conns: {}",
                    application, thisNodeConnections, nodes, approxTotalConnections, maxTotalConnections);

            if (approxTotalConnections > maxTotalConnections) {
                return false;
            }

            return true;
        } catch (final Exception e) {
            throw new NakadiRuntimeException(e);
        }
    }

    public void trackConnectionsCount(final Client client, final int increment) {
        clientConnections.compute(client.getClientId(),
                (app, conns) -> Math.max(0, (conns == null ? 0 : conns) + increment));
    }

    //
    // In the set/remove/list methods below we shouldn't be using the cache but work with ZK directly instead.  These
    // methods are rarely called (only by interacting with the settings API) and cache effects can be confusing there.
    //
    public void set(final String application, final Optional<ApplicationConfig> appConfig) {
        final CuratorFramework curator = zooKeeperHolder.get();
        final String applicationPath = makeApplicationPath(application);
        AuditLogEntry oldEntry = null;
        final AuditLogEntry newEntry = new AuditLogEntry(application, appConfig);
        try {
            final byte[] configBytes = appConfig
                    .map(this::makeBytesFromApplicationConfig)
                    .orElse(null);
            try {
                curator.create()
                        .creatingParentsIfNeeded()
                        .forPath(applicationPath, configBytes);
            } catch (final KeeperException.NodeExistsException e) {
                // fetch old config before overwriting it
                final byte[] oldConfigBytes = curator.getData()
                        .forPath(applicationPath);
                final Optional<ApplicationConfig> oldConfig = makeApplicationConfigFromBytes(oldConfigBytes);
                oldEntry = new AuditLogEntry(application, oldConfig);

                // set new config
                curator.setData()
                        .forPath(applicationPath, configBytes);
            }
        } catch (final Exception e) {
            throw new NakadiRuntimeException("Issue occurred while creating node in zk: " + applicationPath, e);
        }

        auditLogPublisher.publish(
                Optional.ofNullable(oldEntry),
                Optional.of(newEntry),
                NakadiAuditLogPublisher.ResourceType.BLACKLIST_ENTRY,
                oldEntry == null
                    ? NakadiAuditLogPublisher.ActionType.CREATED
                    : NakadiAuditLogPublisher.ActionType.UPDATED,
                application);
    }

    public void remove(final String application) {
        final CuratorFramework curator = zooKeeperHolder.get();
        final String applicationPath = makeApplicationPath(application);
        try {
            final byte[] oldConfigBytes = curator.getData()
                    .forPath(applicationPath);
            final Optional<ApplicationConfig> appConfig = makeApplicationConfigFromBytes(oldConfigBytes);

            curator.delete()
                    .forPath(applicationPath);

            // danger to lose the audit event fixme
            auditLogPublisher.publish(
                    Optional.of(new AuditLogEntry(application, appConfig)),
                    Optional.empty(),
                    NakadiAuditLogPublisher.ResourceType.BLACKLIST_ENTRY, // WTF BlackList?
                    NakadiAuditLogPublisher.ActionType.DELETED,
                    application);

        } catch (final KeeperException.NoNodeException e) {
            LOG.info("Node already deleted or didn't exist in zk: {}", applicationPath);
        } catch (final Exception e) {
            throw new NakadiRuntimeException("Issue occurred while deleting node from zk: " + applicationPath, e);
        }
    }

    public Map<String, Optional<ApplicationConfig>> list() {
        final CuratorFramework curator = zooKeeperHolder.get();
        try {
            return curator.getChildren()
                    .forPath(PATH_ALLOWLIST)
                    .stream()
                    .collect(Collectors.toMap(
                                    Function.identity(),
                                    application -> fetchApplicationConfig(curator, application)));
        } catch (final Exception e) {
            throw new NakadiRuntimeException("Issue occurred while listing nodes from zk: " + PATH_ALLOWLIST, e);
        }
    }

    private String makeApplicationPath(final String application) {
        return String.format("%s/%s", PATH_ALLOWLIST, application);
    }

    private Optional<ApplicationConfig> fetchApplicationConfig(
            final CuratorFramework curator, final String application) {

        final String applicationPath = makeApplicationPath(application);
        try {
            return makeApplicationConfigFromBytes(
                    curator.getData().forPath(applicationPath));
        } catch (final Exception e) {
            throw new NakadiRuntimeException("Issue occurred while fetching node from zk: " + applicationPath, e);
        }
    }

    private Optional<ApplicationConfig> getCachedApplicationConfig(final String application) {
        return Optional.ofNullable(
                allowListCache.getCurrentData(
                        makeApplicationPath(application)))
                .map(ChildData::getData)
                .flatMap(this::makeApplicationConfigFromBytes);
    }

    private Optional<ApplicationConfig> makeApplicationConfigFromBytes(final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(bytes, ApplicationConfig.class));
        } catch (final Exception e) {
            LOG.warn("Failed to parse application configuration from: {}", bytes, e);
            return Optional.empty();
        }
    }

    private byte[] makeBytesFromApplicationConfig(final ApplicationConfig appConfig) {
        try {
            return objectMapper.writeValueAsBytes(appConfig);
        } catch (final Exception e) {
            throw new NakadiRuntimeException("Failed to serialze: " + appConfig, e);
        }
    }

    public static class ApplicationConfig {
        private int maxTotalConnections;

        public ApplicationConfig() {
            this.maxTotalConnections = 0;
        }

        public int getMaxTotalConnections() {
            return maxTotalConnections;
        }

        public void setMaxTotalConnections(final int maxTotalConnections) {
            this.maxTotalConnections = maxTotalConnections;
        }
    }

    public static class AuditLogEntry {
        public final String application;
        public final Optional<ApplicationConfig> configuration;

        private AuditLogEntry(final String application, final Optional<ApplicationConfig> configuration) {
            this.application = application;
            this.configuration = configuration;
        }
    }
}
