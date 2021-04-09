package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.coordinator.IngestorSnapshotClient;
import com.alibaba.maxgraph.v2.coordinator.IngestorWriteSnapshotIdNotifier;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class IngestorWriteSnapshotIdNotifierTest {

    @Test
    void testNotifier() {
        Configs configs = Configs.newBuilder()
                .put(CommonConfig.INGESTOR_NODE_COUNT.getKey(), "1")
                .build();
        RoleClients<IngestorSnapshotClient> roleClients = mock(RoleClients.class);
        IngestorSnapshotClient ingestorSnapshotClient = mock(IngestorSnapshotClient.class);
        when(roleClients.getClient(0)).thenReturn(ingestorSnapshotClient);

        IngestorWriteSnapshotIdNotifier notifier = new IngestorWriteSnapshotIdNotifier(configs, roleClients);
        notifier.notifyWriteSnapshotIdChanged(10L);
        verify(ingestorSnapshotClient).advanceIngestSnapshotId(eq(10L), any());
    }
}
