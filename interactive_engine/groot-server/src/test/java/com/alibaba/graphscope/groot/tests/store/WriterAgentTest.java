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
package com.alibaba.graphscope.groot.tests.store;

import static org.mockito.Mockito.*;

import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.store.SnapshotCommitClient;
import com.alibaba.graphscope.groot.store.StoreService;
import com.alibaba.graphscope.groot.store.WriterAgent;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class WriterAgentTest {

    @Test
    void testWriterAgent() throws InterruptedException, ExecutionException {
        Configs configs = Configs.newBuilder().put(CommonConfig.NODE_IDX.getKey(), "0").build();
        StoreService mockStoreService = mock(StoreService.class);

        MetaService mockMetaService = mock(MetaService.class);
        when(mockMetaService.getQueueCount()).thenReturn(1);

        RoleClients<SnapshotCommitClient> mockSnapshotCommitter = mock(RoleClients.class);

        WriterAgent writerAgent =
                new WriterAgent(configs, mockStoreService, mockMetaService, mockSnapshotCommitter);

        writerAgent.start();

        StoreDataBatch storeDataBatch =
                StoreDataBatch.newBuilder().snapshotId(2L).queueId(0).offset(10L).build();
        writerAgent.writeStore(storeDataBatch);

        verify(mockStoreService, timeout(5000L).times(1)).batchWrite(storeDataBatch);
        verify(mockSnapshotCommitter, timeout(5000L).times(1))
                .getClient(0)
                .commitSnapshotId(0, 1L, 0L, Collections.singletonList(10L));

        writerAgent.stop();
    }
}
