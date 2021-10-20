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
package com.alibaba.maxgraph.tests.common;

import com.alibaba.graphscope.groot.meta.DefaultMetaService;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.maxgraph.common.config.Configs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DefaultMetaServiceTest {

    @Test
    void testMeta() {
        Configs configs =
                Configs.newBuilder()
                        .put("partition.count", "10")
                        .put("ingestor.queue.count", "3")
                        .put("store.node.count", "4")
                        .build();
        /**
         * Partitions assignment
         *
         * <p>| storeID | Partitions | | 0 | 0, 1, 2 | | 1 | 3, 4, 5 | | 2 | 6, 7 | | 3 | 8, 9 |
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
                () -> assertEquals(metaService.getIngestorIdForQueue(1), 1));

        metaService.stop();
    }
}
