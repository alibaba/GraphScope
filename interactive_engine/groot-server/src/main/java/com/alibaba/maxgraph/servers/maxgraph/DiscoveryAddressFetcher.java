/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.servers.maxgraph;

import com.alibaba.maxgraph.common.rpc.RpcAddress;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.graphscope.groot.discovery.MaxGraphNode;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.maxgraph.common.RoleType;

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
