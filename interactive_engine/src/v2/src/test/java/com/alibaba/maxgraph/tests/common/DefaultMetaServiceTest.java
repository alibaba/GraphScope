package com.alibaba.maxgraph.tests.common;

import com.alibaba.maxgraph.v2.common.DefaultMetaService;
import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.config.Configs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DefaultMetaServiceTest {

    @Test
    void testMeta() {
        Configs configs = Configs.newBuilder()
                .put("partition.count", "10")
                .put("ingestor.queue.count", "3")
                .put("store.node.count", "4")
                .build();
        /**
         * Partitions assignment
         *
         * | storeID |  Partitions |
         * | 0       | 0, 1, 2     |
         * | 1       | 3, 4, 5     |
         * | 2       | 6, 7        |
         * | 3       | 8, 9        |
         */
        MetaService metaService = new DefaultMetaService(configs);
        metaService.start();

        Assertions.assertAll(
                () -> assertEquals(metaService.getPartitionCount(), 10),
                () -> assertEquals(metaService.getStoreIdByPartition(0), 0),
                () -> assertEquals(metaService.getStoreIdByPartition(5), 1),
                () -> assertEquals(metaService.getStoreIdByPartition(6), 2),
                () -> assertEquals(metaService.getStoreIdByPartition(8), 3),
                () -> assertEquals(metaService.getPartitionsByStoreId(2), Arrays.asList(6, 7)),
                () -> assertEquals(metaService.getQueueCount(), 3),
                () -> assertEquals(metaService.getQueueIdsForIngestor(2), Arrays.asList(2)),
                () -> assertEquals(metaService.getIngestorIdForQueue(1), 1)
        );

        metaService.stop();
    }
}
