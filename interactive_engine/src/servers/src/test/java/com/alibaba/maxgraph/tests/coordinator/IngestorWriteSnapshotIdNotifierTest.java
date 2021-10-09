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
package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.coordinator.IngestorSnapshotClient;
import com.alibaba.graphscope.groot.coordinator.IngestorWriteSnapshotIdNotifier;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class IngestorWriteSnapshotIdNotifierTest {

    @Test
    void testNotifier() {
        Configs configs =
                Configs.newBuilder().put(CommonConfig.INGESTOR_NODE_COUNT.getKey(), "1").build();
        RoleClients<IngestorSnapshotClient> roleClients = mock(RoleClients.class);
        IngestorSnapshotClient ingestorSnapshotClient = mock(IngestorSnapshotClient.class);
        when(roleClients.getClient(0)).thenReturn(ingestorSnapshotClient);

        IngestorWriteSnapshotIdNotifier notifier =
                new IngestorWriteSnapshotIdNotifier(configs, roleClients);
        notifier.notifyWriteSnapshotIdChanged(10L);
        verify(ingestorSnapshotClient).advanceIngestSnapshotId(eq(10L), any());
    }
}
