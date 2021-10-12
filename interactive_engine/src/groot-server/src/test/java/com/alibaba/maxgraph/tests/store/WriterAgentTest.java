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
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.StoreConfig;
import com.alibaba.graphscope.groot.store.SnapshotCommitter;
import com.alibaba.graphscope.groot.store.StoreService;
import com.alibaba.graphscope.groot.store.WriterAgent;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class WriterAgentTest {

    @Test
    void testWriterAgent() throws InterruptedException, ExecutionException {
        Configs configs =
                Configs.newBuilder()
                        .put(CommonConfig.NODE_IDX.getKey(), "0")
                        .put(StoreConfig.STORE_COMMIT_INTERVAL_MS.getKey(), "10")
                        .build();
        StoreService mockStoreService = mock(StoreService.class);

        MetaService mockMetaService = mock(MetaService.class);
        when(mockMetaService.getQueueCount()).thenReturn(1);

        SnapshotCommitter mockSnapshotCommitter = mock(SnapshotCommitter.class);

        WriterAgent writerAgent =
                new WriterAgent(configs, mockStoreService, mockMetaService, mockSnapshotCommitter);
        writerAgent.init(0L);

        writerAgent.start();

        StoreDataBatch storeDataBatch =
                StoreDataBatch.newBuilder().snapshotId(2L).queueId(0).offset(10L).build();
        writerAgent.writeStore(storeDataBatch);

        verify(mockStoreService, timeout(5000L).times(1)).batchWrite(storeDataBatch);
        verify(mockSnapshotCommitter, timeout(5000L).times(1))
                .commitSnapshotId(0, 1L, 0L, Collections.singletonList(10L));

        writerAgent.stop();
    }
}
