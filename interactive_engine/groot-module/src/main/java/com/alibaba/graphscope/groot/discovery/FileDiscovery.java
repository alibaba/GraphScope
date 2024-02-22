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

import static com.alibaba.graphscope.groot.common.RoleType.*;

import com.alibaba.graphscope.groot.Utils;
import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FileDiscovery implements NodeDiscovery {
    private final Logger logger = LoggerFactory.getLogger(FileDiscovery.class);

    private final Map<RoleType, Map<Integer, GrootNode>> allNodes = new HashMap<>();
    private boolean started = false;

    Configs configs;

    public FileDiscovery(Configs configs) {
        this.configs = configs;
        // Store related nodes
        String storePrefix = DiscoveryConfig.DNS_NAME_PREFIX_STORE.get(configs);
        String frontendPrefix = DiscoveryConfig.DNS_NAME_PREFIX_FRONTEND.get(configs);
        String coordinatorPrefix = DiscoveryConfig.DNS_NAME_PREFIX_COORDINATOR.get(configs);

        // Frontend nodes
        int frontendCount = CommonConfig.FRONTEND_NODE_COUNT.get(configs);
        Map<Integer, GrootNode> frontendNodes =
                makeRoleNodes(frontendCount, frontendPrefix, FRONTEND);
        this.allNodes.put(FRONTEND, frontendNodes);

        // Coordinator nodes
        int coordinatorCount = CommonConfig.COORDINATOR_NODE_COUNT.get(configs);
        Map<Integer, GrootNode> coordinatorNodes =
                makeRoleNodes(coordinatorCount, coordinatorPrefix, COORDINATOR);
        this.allNodes.put(COORDINATOR, coordinatorNodes);

        int storeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        Map<Integer, GrootNode> storeNodes = makeRoleNodes(storeCount, storePrefix, STORE);
        this.allNodes.put(STORE, storeNodes);

        Map<Integer, GrootNode> gaiaRpcNodes = makeRoleNodes(storeCount, storePrefix, GAIA_RPC);
        this.allNodes.put(GAIA_RPC, gaiaRpcNodes);

        Map<Integer, GrootNode> gaiaEngineNodes =
                makeRoleNodes(storeCount, storePrefix, GAIA_ENGINE);
        this.allNodes.put(GAIA_ENGINE, gaiaEngineNodes);
    }

    @Override
    public void start() {
        if (!started) {
            this.started = true;
        }
    }

    private Map<Integer, GrootNode> makeRoleNodes(int nodeCount, String namePrefix, RoleType role) {
        Map<Integer, GrootNode> nodes = new HashMap<>();
        for (int i = 0; i < nodeCount; i++) {
            int port = Utils.getPort(configs, role, i);
            String host = namePrefix.replace("{}", String.valueOf(i));
            nodes.put(i, new GrootNode(role.getName(), i, host, port));
        }
        return nodes;
    }

    @Override
    public void stop() {
        this.started = false;
    }

    @Override
    public void addListener(Listener listener) {
        for (Map.Entry<RoleType, Map<Integer, GrootNode>> e : allNodes.entrySet()) {
            RoleType role = e.getKey();
            Map<Integer, GrootNode> nodes = e.getValue();
            if (nodes.isEmpty()) {
                continue;
            }
            try {
                listener.nodesJoin(role, nodes);
            } catch (Exception ex) {
                logger.error("listener {} failed on nodesJoin {}", listener, nodes, ex);
            }
        }
    }

    @Override
    public void removeListener(Listener listener) {}

    @Override
    public GrootNode getLocalNode() {
        return null;
    }
}
