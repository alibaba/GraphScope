package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.v2.common.discovery.MaxGraphNode;
import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;

import java.util.HashMap;
import java.util.Map;

public class SnapshotNotifier implements NodeDiscovery.Listener {

    private NodeDiscovery nodeDiscovery;
    private SnapshotManager snapshotManager;
    private SchemaManager schemaManager;
    private RoleClients<FrontendSnapshotClient> frontendSnapshotClients;

    private Map<Integer, QuerySnapshotListener> listeners;

    public SnapshotNotifier(NodeDiscovery nodeDiscovery, SnapshotManager snapshotManager,
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
        nodes.forEach((id, node) -> {
            QuerySnapshotListener notifyFrontendListener =
                    new NotifyFrontendListener(id, frontendSnapshotClients.getClient(id), this.schemaManager);
            this.snapshotManager.addListener(notifyFrontendListener);
            this.listeners.put(id, notifyFrontendListener);
        });
    }

    @Override
    public void nodesLeft(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role != RoleType.FRONTEND) {
            return;
        }
        nodes.forEach((id, node) -> {
            QuerySnapshotListener removeListener = listeners.remove(id);
            this.snapshotManager.removeListener(removeListener);
        });
    }
}
