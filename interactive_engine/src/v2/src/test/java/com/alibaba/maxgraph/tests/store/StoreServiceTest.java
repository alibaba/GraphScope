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
package com.alibaba.maxgraph.tests.store;

import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.graphscope.groot.store.GraphPartition;
import com.alibaba.graphscope.groot.store.StoreService;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class StoreServiceTest {

    @Test
    void testStoreService() throws IOException, InterruptedException, ExecutionException {
        Configs configs = Configs.newBuilder().put(CommonConfig.NODE_IDX.getKey(), "0").build();

        MetaService mockMetaService = mock(MetaService.class);
        when(mockMetaService.getPartitionsByStoreId(0)).thenReturn(Arrays.asList(0));

        StoreService spyStoreService = spy(new StoreService(configs, mockMetaService));

        GraphPartition mockGraphPartition = mock(GraphPartition.class);
        when(mockGraphPartition.recover()).thenReturn(10L);
        doReturn(mockGraphPartition).when(spyStoreService).makeGraphPartition(any(), eq(0));

        spyStoreService.start();
        assertEquals(spyStoreService.recover(), 10L);

        StoreDataBatch storeDataBatch =
                StoreDataBatch.newBuilder()
                        .snapshotId(20L)
                        .addOperation(0, OperationBlob.MARKER_OPERATION_BLOB)
                        .build();
        spyStoreService.batchWrite(storeDataBatch);
        verify(mockGraphPartition, timeout(100L))
                .writeBatch(
                        20L,
                        OperationBatch.newBuilder()
                                .addOperationBlob(OperationBlob.MARKER_OPERATION_BLOB)
                                .build());
        spyStoreService.stop();
        verify(mockGraphPartition).close();
    }
}
