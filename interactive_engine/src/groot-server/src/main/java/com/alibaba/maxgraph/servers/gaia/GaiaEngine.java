package com.alibaba.maxgraph.servers.gaia;

import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.graphscope.groot.discovery.*;
import com.alibaba.maxgraph.compiler.api.exception.MaxGraphException;
import com.alibaba.graphscope.groot.store.GraphPartition;
import com.alibaba.maxgraph.servers.jna.GaiaLibrary;
import com.alibaba.maxgraph.servers.jna.GaiaPortsResponse;
import com.alibaba.graphscope.groot.store.jna.JnaGraphStore;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class GaiaEngine implements ExecutorEngine {
    private static final Logger logger = LoggerFactory.getLogger(GaiaEngine.class);

    private Configs configs;
    private Pointer pointer;
    private NodeDiscovery engineDiscovery;
    private NodeDiscovery rpcDiscovery;
    private LocalNodeProvider engineNodeProvider;
    private LocalNodeProvider rpcNodeProvider;
    private int nodeCount;

    private Map<Integer, MaxGraphNode> engineNodes = new ConcurrentHashMap<>();

    public GaiaEngine(Configs configs, DiscoveryFactory discoveryFactory) {
        this.configs = configs;
        this.engineNodeProvider = new LocalNodeProvider(RoleType.GAIA_ENGINE, this.configs);
        this.rpcNodeProvider = new LocalNodeProvider(RoleType.GAIA_RPC, this.configs);
        this.engineDiscovery = discoveryFactory.makeDiscovery(this.engineNodeProvider);
        this.rpcDiscovery = discoveryFactory.makeDiscovery(this.rpcNodeProvider);
        this.nodeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
    }

    @Override
    public void init() {
        Configs engineConfigs =
                Configs.newBuilder(this.configs)
                        .put(
                                "worker.num",
                                String.valueOf(CommonConfig.STORE_NODE_COUNT.get(this.configs)))
                        .build();
        byte[] configBytes = engineConfigs.toProto().toByteArray();
        this.pointer = GaiaLibrary.INSTANCE.initialize(configBytes, configBytes.length);
    }

    @Override
    public void addPartition(GraphPartition partition) {
        int partitionId = partition.getId();
        if (partition instanceof JnaGraphStore) {
            GaiaLibrary.INSTANCE.addPartition(
                    this.pointer, partitionId, ((JnaGraphStore) partition).getPointer());
        }
    }

    @Override
    public void updatePartitionRouting(int partitionId, int serverId) {
        GaiaLibrary.INSTANCE.updatePartitionRouting(this.pointer, partitionId, serverId);
    }

    @Override
    public void start() {
        try (GaiaPortsResponse gaiaPortsResponse = GaiaLibrary.INSTANCE.startEngine(this.pointer)) {
            if (!gaiaPortsResponse.success) {
                throw new MaxGraphException(gaiaPortsResponse.errMsg);
            }
            engineNodeProvider.apply(gaiaPortsResponse.enginePort);
            rpcNodeProvider.apply(gaiaPortsResponse.rpcPort);
        }

        this.engineDiscovery.start();
        this.engineDiscovery.addListener(this);
        this.rpcDiscovery.start();
    }

    @Override
    public void stop() {
        this.engineDiscovery.removeListener(this);
        this.engineDiscovery.stop();
        this.rpcDiscovery.stop();
        GaiaLibrary.INSTANCE.stopEngine(this.pointer);
    }

    @Override
    public void nodesJoin(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role == RoleType.GAIA_ENGINE) {
            this.engineNodes.putAll(nodes);
            if (this.engineNodes.size() == this.nodeCount) {
                String peerViewString =
                        nodes.values().stream()
                                .map(
                                        n ->
                                                String.format(
                                                        "%s#%s:%s",
                                                        n.getIdx(), n.getHost(), n.getPort()))
                                .collect(Collectors.joining(","));
                logger.info("updatePeerView [" + peerViewString + "]");
                GaiaLibrary.INSTANCE.updatePeerView(this.pointer, peerViewString);
            }
        }
    }

    @Override
    public void nodesLeft(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role == RoleType.GAIA_ENGINE) {
            nodes.keySet().forEach(k -> this.engineNodes.remove(k));
        }
    }
}
