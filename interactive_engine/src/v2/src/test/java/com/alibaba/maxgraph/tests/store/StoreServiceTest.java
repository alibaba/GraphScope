package com.alibaba.maxgraph.tests.store;

import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.OperationBlob;
import com.alibaba.maxgraph.v2.common.StoreDataBatch;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.store.GraphPartition;
import com.alibaba.maxgraph.v2.store.StoreService;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class StoreServiceTest {

    @Test
    void testStoreService() throws IOException, InterruptedException, ExecutionException {
        Configs configs = Configs.newBuilder()
                .put(CommonConfig.NODE_IDX.getKey(), "0")
                .build();

        MetaService mockMetaService = mock(MetaService.class);
        when(mockMetaService.getPartitionsByStoreId(0)).thenReturn(Arrays.asList(0));

        StoreService spyStoreService = spy(new StoreService(configs, mockMetaService));

        GraphPartition mockGraphPartition = mock(GraphPartition.class);
        when(mockGraphPartition.recover()).thenReturn(10L);
        doReturn(mockGraphPartition).when(spyStoreService).makeGraphPartition(any(), eq(0));

        spyStoreService.start();
        assertEquals(spyStoreService.recover(), 10L);

        StoreDataBatch storeDataBatch = StoreDataBatch.newBuilder()
                .snapshotId(20L)
                .addOperation(0, OperationBlob.MARKER_OPERATION_BLOB)
                .build();
        spyStoreService.batchWrite(storeDataBatch);
        verify(mockGraphPartition, timeout(100L)).writeBatch(20L,
                OperationBatch.newBuilder().addOperationBlob(OperationBlob.MARKER_OPERATION_BLOB).build());
        spyStoreService.stop();
        verify(mockGraphPartition).close();
    }
}
