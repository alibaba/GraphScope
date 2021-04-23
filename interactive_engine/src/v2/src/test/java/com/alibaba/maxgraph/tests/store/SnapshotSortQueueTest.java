package com.alibaba.maxgraph.tests.store;

import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.StoreDataBatch;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.store.SnapshotSortQueue;
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
         * Q1: 4, 5, 7, 7
         * Q0: 4, 5, 6, 7
         *
         * (Q1, 4) (Q0, 4) (Q1, 5) (Q0, 5) (Q0, 6) (Q1, 7) (Q1, 7) (Q0, 7)
         */
        snapshotSortQueue.offerQueue(1, StoreDataBatch.newBuilder().snapshotId(4L).queueId(1).build());
        snapshotSortQueue.offerQueue(1, StoreDataBatch.newBuilder().snapshotId(5L).queueId(1).build());
        snapshotSortQueue.offerQueue(1, StoreDataBatch.newBuilder().snapshotId(7L).queueId(1).build());
        snapshotSortQueue.offerQueue(1, StoreDataBatch.newBuilder().snapshotId(7L).queueId(1).build());
        // For end snapshot 7
        snapshotSortQueue.offerQueue(1, StoreDataBatch.newBuilder().snapshotId(100L).queueId(1).build());
        snapshotSortQueue.offerQueue(0, StoreDataBatch.newBuilder().snapshotId(4L).queueId(0).build());
        snapshotSortQueue.offerQueue(0, StoreDataBatch.newBuilder().snapshotId(5L).queueId(0).build());
        snapshotSortQueue.offerQueue(0, StoreDataBatch.newBuilder().snapshotId(6L).queueId(0).build());
        snapshotSortQueue.offerQueue(0, StoreDataBatch.newBuilder().snapshotId(7L).queueId(0).build());

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
