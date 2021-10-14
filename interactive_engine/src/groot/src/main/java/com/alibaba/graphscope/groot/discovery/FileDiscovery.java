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
package com.alibaba.graphscope.groot.discovery;

import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.maxgraph.common.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FileDiscovery implements NodeDiscovery {
    private Logger logger = LoggerFactory.getLogger(FileDiscovery.class);

    private Configs configs;
    private Map<RoleType, Map<Integer, MaxGraphNode>> allNodes = new HashMap<>();
    private boolean started = false;

    public FileDiscovery(Configs configs) {
        this.configs = configs;
        int storeCount = CommonConfig.STORE_NODE_COUNT.get(this.configs);
        String storeNamePrefix = DiscoveryConfig.DNS_NAME_PREFIX_STORE.get(this.configs);
        int port = CommonConfig.RPC_PORT.get(this.configs);
        Map<Integer, MaxGraphNode> storeNodes =
                makeRoleNodes(storeCount, storeNamePrefix, RoleType.STORE.getName(), port);
        this.allNodes.put(RoleType.STORE, storeNodes);

        int graphPort = StoreConfig.EXECUTOR_GRAPH_PORT.get(this.configs);
        Map<Integer, MaxGraphNode> graphNodes =
                makeRoleNodes(
                        storeCount, storeNamePrefix, RoleType.EXECUTOR_GRAPH.getName(), graphPort);
        this.allNodes.put(RoleType.EXECUTOR_GRAPH, graphNodes);

        int queryPort = StoreConfig.EXECUTOR_QUERY_PORT.get(this.configs);
        Map<Integer, MaxGraphNode> queryNodes =
                makeRoleNodes(
                        storeCount, storeNamePrefix, RoleType.EXECUTOR_QUERY.getName(), queryPort);
        this.allNodes.put(RoleType.EXECUTOR_QUERY, queryNodes);

        int enginePort = StoreConfig.EXECUTOR_ENGINE_PORT.get(this.configs);
        Map<Integer, MaxGraphNode> engineNodes =
                makeRoleNodes(
                        storeCount,
                        storeNamePrefix,
                        RoleType.EXECUTOR_ENGINE.getName(),
                        enginePort);
        this.allNodes.put(RoleType.EXECUTOR_ENGINE, engineNodes);

        int gaiaRpcPort = GaiaConfig.GAIA_RPC_PORT.get(this.configs);
        Map<Integer, MaxGraphNode> gaiaRpcNodes =
                makeRoleNodes(
                        storeCount, storeNamePrefix, RoleType.GAIA_RPC.getName(), gaiaRpcPort);
        this.allNodes.put(RoleType.GAIA_RPC, gaiaRpcNodes);

        int gaiaEnginePort = GaiaConfig.GAIA_ENGINE_PORT.get(this.configs);
        Map<Integer, MaxGraphNode> gaiaEngineNodes =
                makeRoleNodes(
                        storeCount,
                        storeNamePrefix,
                        RoleType.GAIA_ENGINE.getName(),
                        gaiaEnginePort);
        this.allNodes.put(RoleType.GAIA_ENGINE, gaiaEngineNodes);

        int frontendCount = CommonConfig.FRONTEND_NODE_COUNT.get(this.configs);
        String frontendNamePrefix = DiscoveryConfig.DNS_NAME_PREFIX_FRONTEND.get(this.configs);
        Map<Integer, MaxGraphNode> frontendNodes =
                makeRoleNodes(frontendCount, frontendNamePrefix, RoleType.FRONTEND.getName(), port);
        this.allNodes.put(RoleType.FRONTEND, frontendNodes);

        int ingestorCount = CommonConfig.INGESTOR_NODE_COUNT.get(this.configs);
        String ingestorNamePrefix = DiscoveryConfig.DNS_NAME_PREFIX_INGESTOR.get(this.configs);
        Map<Integer, MaxGraphNode> ingestorNodes =
                makeRoleNodes(ingestorCount, ingestorNamePrefix, RoleType.INGESTOR.getName(), port);
        this.allNodes.put(RoleType.INGESTOR, ingestorNodes);

        int coordinatorCount = CommonConfig.COORDINATOR_NODE_COUNT.get(this.configs);
        String coordinatorNamePrefix =
                DiscoveryConfig.DNS_NAME_PREFIX_COORDINATOR.get(this.configs);
        Map<Integer, MaxGraphNode> coordinatorNodes =
                makeRoleNodes(
                        coordinatorCount,
                        coordinatorNamePrefix,
                        RoleType.COORDINATOR.getName(),
                        port);
        this.allNodes.put(RoleType.COORDINATOR, coordinatorNodes);
    }

    @Override
    public void start() {
        if (!started) {
            this.started = true;
        }
    }

    private Map<Integer, MaxGraphNode> makeRoleNodes(
            int storeCount, String namePrefix, String role, int port) {
        Map<Integer, MaxGraphNode> nodes = new HashMap<>();
        for (int i = 0; i < storeCount; i++) {
            nodes.put(
                    i,
                    new MaxGraphNode(role, i, namePrefix.replace("{}", String.valueOf(i)), port));
        }
        return nodes;
    }

    @Override
    public void stop() {
        this.started = false;
    }

    @Override
    public void addListener(Listener listener) {
        for (Map.Entry<RoleType, Map<Integer, MaxGraphNode>> e : allNodes.entrySet()) {
            RoleType role = e.getKey();
            Map<Integer, MaxGraphNode> nodes = e.getValue();
            if (!nodes.isEmpty()) {
                try {
                    listener.nodesJoin(role, nodes);
                } catch (Exception ex) {
                    logger.error(
                            "listener [" + listener + "] failed on nodesJoin [" + nodes + "]", ex);
                }
            }
        }
    }

    @Override
    public void removeListener(Listener listener) {}

    @Override
    public MaxGraphNode getLocalNode() {
        return null;
    }
}
