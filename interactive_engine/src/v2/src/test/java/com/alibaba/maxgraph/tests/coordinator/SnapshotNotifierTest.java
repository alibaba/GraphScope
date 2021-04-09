package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.MaxGraphNode;
import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.coordinator.FrontendSnapshotClient;
import com.alibaba.maxgraph.v2.coordinator.NotifyFrontendListener;
import com.alibaba.maxgraph.v2.coordinator.SnapshotManager;
import com.alibaba.maxgraph.v2.coordinator.SnapshotNotifier;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import static org.mockito.Mockito.*;

public class SnapshotNotifierTest {

    @Test
    void testSnapshotNotifier() {
        NodeDiscovery discovery = mock(NodeDiscovery.class);
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        RoleClients<FrontendSnapshotClient> roleClients = mock(RoleClients.class);
        SnapshotNotifier snapshotNotifier = new SnapshotNotifier(discovery, snapshotManager, null, roleClients);
        snapshotNotifier.start();

        MaxGraphNode localNode = MaxGraphNode.createLocalNode(Configs.newBuilder()
                .put(CommonConfig.ROLE_NAME.getKey(), RoleType.COORDINATOR.getName()).build(), 1111);
        snapshotNotifier.nodesJoin(RoleType.FRONTEND, Collections.singletonMap(1, localNode));
        ArgumentCaptor<NotifyFrontendListener> captor = ArgumentCaptor.forClass(NotifyFrontendListener.class);
        verify(snapshotManager).addListener(captor.capture());
        NotifyFrontendListener listener = captor.getValue();
        snapshotNotifier.nodesLeft(RoleType.FRONTEND, Collections.singletonMap(1, localNode));
        verify(snapshotManager).removeListener(listener);
    }
}
