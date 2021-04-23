package com.alibaba.maxgraph.v2.store.executor;

import com.alibaba.maxgraph.proto.v2.EngineServerAddresses;
import com.alibaba.maxgraph.v2.common.discovery.MaxGraphNode;
import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EngineServerListener implements NodeDiscovery.Listener {
    private static final Logger logger = LoggerFactory.getLogger(EngineServerListener.class);
    private ExecutorManager executorManager;
    private Map<Integer, MaxGraphNode> nodes = Maps.newHashMap();

    public EngineServerListener(ExecutorManager executorManager) {
        this.executorManager = executorManager;
    }

    @Override
    public void nodesJoin(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role == RoleType.EXECUTOR_ENGINE) {
            this.nodes.putAll(nodes);
            if (this.nodes.size() == this.executorManager.getNodeCount()) {
                List<MaxGraphNode> nodeList = Lists.newArrayList(this.nodes.values());
                nodeList.sort(Comparator.comparingInt(MaxGraphNode::getIdx));
                logger.info("All the engine servers are started " + nodeList);
                List<String> addrs = nodeList.stream().map(v -> AddressUtils.joinAddress(v.getHost(), v.getPort()))
                        .collect(Collectors.toList());
                executorManager.connectEngineServerList(addrs);
            } else if (this.nodes.size() > this.executorManager.getNodeCount()) {
                throw new IllegalArgumentException("count of nodes " + this.nodes + " > " + this.executorManager.getNodeCount());
            }
        }
    }

    @Override
    public void nodesLeft(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role == RoleType.EXECUTOR_ENGINE) {
            nodes.keySet().forEach(k -> this.nodes.remove(k));
        }
    }
}
