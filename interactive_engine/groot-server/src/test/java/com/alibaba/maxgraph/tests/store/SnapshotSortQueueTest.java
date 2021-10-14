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
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.graphscope.groot.store.SnapshotSortQueue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotSortQueueTest {

    @Test
    void testQueue() throws InterruptedException {
        Configs configs = Configs.newBuilder().build();
        MetaService mockMetaService = mock(MetaService.class);
        when(mockMetaService.getQueueCount()).thenReturn(2);

        SnapshotSortQueue snapshotSortQueue = new SnapshotSortQueue(configs, mockMetaService);
        /**
         * Q1: 4, 5, 7, 7 Q0: 4, 5, 6, 7
         *
         * <p>(Q1, 4) (Q0, 4) (Q1, 5) (Q0, 5) (Q0, 6) (Q1, 7) (Q1, 7) (Q0, 7)
         */
        snapshotSortQueue.offerQueue(
                1, StoreDataBatch.newBuilder().snapshotId(4L).queueId(1).build());
        snapshotSortQueue.offerQueue(
                1, StoreDataBatch.newBuilder().snapshotId(5L).queueId(1).build());
        snapshotSortQueue.offerQueue(
                1, StoreDataBatch.newBuilder().snapshotId(7L).queueId(1).build());
        snapshotSortQueue.offerQueue(
                1, StoreDataBatch.newBuilder().snapshotId(7L).queueId(1).build());
        // For end snapshot 7
        snapshotSortQueue.offerQueue(
                1, StoreDataBatch.newBuilder().snapshotId(100L).queueId(1).build());
        snapshotSortQueue.offerQueue(
                0, StoreDataBatch.newBuilder().snapshotId(4L).queueId(0).build());
        snapshotSortQueue.offerQueue(
                0, StoreDataBatch.newBuilder().snapshotId(5L).queueId(0).build());
        snapshotSortQueue.offerQueue(
                0, StoreDataBatch.newBuilder().snapshotId(6L).queueId(0).build());
        snapshotSortQueue.offerQueue(
                0, StoreDataBatch.newBuilder().snapshotId(7L).queueId(0).build());

        StoreDataBatch entry = snapshotSortQueue.poll();
        assertEquals(entry.getQueueId(), 1);
        assertEquals(entry.getSnapshotId(), 4L);

        entry = snapshotSortQueue.poll();
        assertEquals(entry.getQueueId(), 0);
        assertEquals(entry.getSnapshotId(), 4L);

        entry = snapshotSortQueue.poll();
        assertEquals(entry.getQueueId(), 1);
        assertEquals(entry.getSnapshotId(), 5L);

        entry = snapshotSortQueue.poll();
        assertEquals(entry.getQueueId(), 0);
        assertEquals(entry.getSnapshotId(), 5L);

        entry = snapshotSortQueue.poll();
        assertEquals(entry.getQueueId(), 0);
        assertEquals(entry.getSnapshotId(), 6L);

        entry = snapshotSortQueue.poll();
        assertEquals(entry.getQueueId(), 1);
        assertEquals(entry.getSnapshotId(), 7L);

        entry = snapshotSortQueue.poll();
        assertEquals(entry.getQueueId(), 1);
        assertEquals(entry.getSnapshotId(), 7L);

        entry = snapshotSortQueue.poll();
        assertEquals(entry.getQueueId(), 0);
        assertEquals(entry.getSnapshotId(), 7L);
    }
}
