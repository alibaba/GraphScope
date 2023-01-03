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
package com.alibaba.graphscope.groot.tests.coordinator;

import static org.mockito.Mockito.*;

import com.alibaba.graphscope.common.RoleType;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.coordinator.FrontendSnapshotClient;
import com.alibaba.graphscope.groot.coordinator.NotifyFrontendListener;
import com.alibaba.graphscope.groot.coordinator.SnapshotManager;
import com.alibaba.graphscope.groot.coordinator.SnapshotNotifier;
import com.alibaba.graphscope.groot.discovery.GrootNode;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.graphscope.groot.rpc.RoleClients;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

public class SnapshotNotifierTest {

    @Test
    void testSnapshotNotifier() {
        NodeDiscovery discovery = mock(NodeDiscovery.class);
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        RoleClients<FrontendSnapshotClient> roleClients = mock(RoleClients.class);
        SnapshotNotifier snapshotNotifier =
                new SnapshotNotifier(discovery, snapshotManager, null, roleClients);
        snapshotNotifier.start();

        GrootNode localNode =
                GrootNode.createLocalNode(
                        Configs.newBuilder()
                                .put(
                                        CommonConfig.ROLE_NAME.getKey(),
                                        RoleType.COORDINATOR.getName())
                                .build(),
                        1111);
        snapshotNotifier.nodesJoin(RoleType.FRONTEND, Collections.singletonMap(1, localNode));
        ArgumentCaptor<NotifyFrontendListener> captor =
                ArgumentCaptor.forClass(NotifyFrontendListener.class);
        verify(snapshotManager).addListener(captor.capture());
        NotifyFrontendListener listener = captor.getValue();
        snapshotNotifier.nodesLeft(RoleType.FRONTEND, Collections.singletonMap(1, localNode));
        verify(snapshotManager).removeListener(listener);
    }
}
