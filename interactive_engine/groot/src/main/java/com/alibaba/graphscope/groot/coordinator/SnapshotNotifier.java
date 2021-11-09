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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.discovery.MaxGraphNode;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.graphscope.groot.rpc.RoleClients;

import java.util.HashMap;
import java.util.Map;

public class SnapshotNotifier implements NodeDiscovery.Listener {

    private NodeDiscovery nodeDiscovery;
    private SnapshotManager snapshotManager;
    private SchemaManager schemaManager;
    private RoleClients<FrontendSnapshotClient> frontendSnapshotClients;

    private Map<Integer, QuerySnapshotListener> listeners;

    public SnapshotNotifier(
            NodeDiscovery nodeDiscovery,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            RoleClients<FrontendSnapshotClient> frontendSnapshotClients) {
        this.nodeDiscovery = nodeDiscovery;
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.frontendSnapshotClients = frontendSnapshotClients;
    }

    public void start() {
        this.listeners = new HashMap<>();
        this.nodeDiscovery.addListener(this);
    }

    public void stop() {
        this.nodeDiscovery.removeListener(this);
    }

    @Override
    public void nodesJoin(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role != RoleType.FRONTEND) {
            return;
        }
        nodes.forEach(
                (id, node) -> {
                    QuerySnapshotListener notifyFrontendListener =
                            new NotifyFrontendListener(
                                    id, frontendSnapshotClients.getClient(id), this.schemaManager);
                    this.snapshotManager.addListener(notifyFrontendListener);
                    this.listeners.put(id, notifyFrontendListener);
                });
    }

    @Override
    public void nodesLeft(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role != RoleType.FRONTEND) {
            return;
        }
        nodes.forEach(
                (id, node) -> {
                    QuerySnapshotListener removeListener = listeners.remove(id);
                    this.snapshotManager.removeListener(removeListener);
                });
    }
}
