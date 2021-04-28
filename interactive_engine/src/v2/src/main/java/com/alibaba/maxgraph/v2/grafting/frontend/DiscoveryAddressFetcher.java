package com.alibaba.maxgraph.v2.grafting.frontend;

import com.alibaba.maxgraph.common.rpc.RpcAddress;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.v2.common.discovery.MaxGraphNode;
import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DiscoveryAddressFetcher implements RpcAddressFetcher, NodeDiscovery.Listener {

    private Map<Integer, MaxGraphNode> executorNodes = new ConcurrentHashMap<>();
    private Map<Integer, MaxGraphNode> graphNodes = new ConcurrentHashMap<>();

    public DiscoveryAddressFetcher(NodeDiscovery discovery) {
        discovery.addListener(this);
    }

    @Override
    public List<RpcAddress> getAddressList() {
        List<RpcAddress> rpcAddressList = new ArrayList<>();
        for (MaxGraphNode maxGraphNode : graphNodes.values()) {
            rpcAddressList.add(new RpcAddress(maxGraphNode.getHost(), maxGraphNode.getPort()));
        }
        return rpcAddressList;
    }

    @Override
    public List<Endpoint> getServiceAddress() {
        List<Endpoint> endpoints = new ArrayList<>();
        for (MaxGraphNode node : executorNodes.values()) {
            endpoints.add(new Endpoint(node.getHost(), 0, 0, node.getPort()));
        }
        return endpoints;
    }

    @Override
    public int getAddressCount() {
        return executorNodes.size();
    }

    @Override
    public void nodesJoin(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role == RoleType.EXECUTOR_QUERY) {
            this.executorNodes.putAll(nodes);
        } else if (role == RoleType.EXECUTOR_GRAPH) {
            this.graphNodes.putAll(nodes);
        }
    }

    @Override
    public void nodesLeft(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role == RoleType.EXECUTOR_QUERY) {
            nodes.keySet().forEach(k -> this.executorNodes.remove(k));
        } else if (role == RoleType.EXECUTOR_GRAPH) {
            nodes.keySet().forEach(k -> this.graphNodes.remove(k));
        }
    }
}
