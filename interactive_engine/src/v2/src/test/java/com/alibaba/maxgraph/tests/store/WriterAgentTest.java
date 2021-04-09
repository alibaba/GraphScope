package com.alibaba.maxgraph.tests.store;

import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.StoreDataBatch;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.StoreConfig;
import com.alibaba.maxgraph.v2.store.SnapshotCommitter;
import com.alibaba.maxgraph.v2.store.StoreService;
import com.alibaba.maxgraph.v2.store.WriterAgent;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class WriterAgentTest {

    @Test
    void testWriterAgent() throws InterruptedException, ExecutionException {
        Configs configs = Configs.newBuilder()
                .put(CommonConfig.NODE_IDX.getKey(), "0")
                .put(StoreConfig.STORE_COMMIT_INTERVAL_MS.getKey(), "10")
                .build();
        StoreService mockStoreService = mock(StoreService.class);

        MetaService mockMetaService = mock(MetaService.class);
        when(mockMetaService.getQueueCount()).thenReturn(1);

        SnapshotCommitter mockSnapshotCommitter = mock(SnapshotCommitter.class);

        WriterAgent writerAgent = new WriterAgent(configs, mockStoreService, mockMetaService, mockSnapshotCommitter);
        writerAgent.init(0L);

        writerAgent.start();

        StoreDataBatch storeDataBatch = StoreDataBatch.newBuilder()
                .snapshotId(2L)
                .queueId(0)
                .offset(10L)
                .build();
        writerAgent.writeStore(storeDataBatch);

        verify(mockStoreService, timeout(5000L).times(1)).batchWrite(storeDataBatch);
        verify(mockSnapshotCommitter, timeout(5000L).times(1)).commitSnapshotId(0, 1L, 0L,
                Collections.singletonList(10L));

        writerAgent.stop();
    }
}
