package com.alibaba.maxgraph.v2.store.executor.gaia;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.*;
import com.alibaba.maxgraph.v2.store.GraphPartition;
import com.alibaba.maxgraph.v2.store.executor.ExecutorEngine;
import com.alibaba.maxgraph.v2.store.executor.jna.GaiaLibrary;
import com.alibaba.maxgraph.v2.store.jna.JnaGraphStore;
import com.sun.jna.Pointer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class GaiaEngine implements ExecutorEngine {

    private Configs configs;
    private Pointer pointer;
    private NodeDiscovery engineDiscovery;
    private LocalNodeProvider localNodeProvider;
    private int nodeCount;

    private Map<Integer, MaxGraphNode> engineNodes = new ConcurrentHashMap<>();

    public GaiaEngine(Configs configs, DiscoveryFactory discoveryFactory) {
        this.configs = configs;
        this.localNodeProvider = new LocalNodeProvider(RoleType.GAIA_ENGINE, this.configs);
        this.engineDiscovery = discoveryFactory.makeDiscovery(this.localNodeProvider);
        this.nodeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
    }

    @Override
    public void init() {
        Configs engineConfigs = Configs.newBuilder(this.configs).build();
        byte[] configBytes = engineConfigs.toProto().toByteArray();
        this.pointer = GaiaLibrary.INSTANCE.initialize(configBytes, configBytes.length);
    }

    @Override
    public void addPartition(GraphPartition partition) {
        int partitionId = partition.getId();
        if (partition instanceof JnaGraphStore) {
            GaiaLibrary.INSTANCE.addPartition(this.pointer, partitionId, ((JnaGraphStore) partition).getPointer());
        }
    }

    @Override
    public void start() {
        int port = GaiaLibrary.INSTANCE.startEngine(this.pointer);
        localNodeProvider.apply(port);
        this.engineDiscovery.start();
        this.engineDiscovery.addListener(this);
    }

    @Override
    public void stop() {
        this.engineDiscovery.removeListener(this);
        this.engineDiscovery.stop();
        GaiaLibrary.INSTANCE.stopEngine(this.pointer);
    }

    @Override
    public void nodesJoin(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role == RoleType.GAIA_ENGINE) {
            this.engineNodes.putAll(nodes);
            if (this.engineNodes.size() == this.nodeCount) {
                String peerViewString = nodes.values().stream()
                        .map(n -> String.format("%s-%s:%s", n.getIdx(), n.getHost(), n.getPort()))
                        .collect(Collectors.joining(","));
                GaiaLibrary.INSTANCE.updatePeerView(peerViewString);
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
