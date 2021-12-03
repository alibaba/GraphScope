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
package com.alibaba.maxgraph.tests.ingestor;

import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.IngestorConfig;
import com.alibaba.graphscope.groot.discovery.MaxGraphNode;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.ingestor.IngestProcessor;
import com.alibaba.graphscope.groot.ingestor.IngestProgressFetcher;
import com.alibaba.graphscope.groot.ingestor.IngestService;
import com.alibaba.graphscope.groot.ingestor.StoreWriter;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.*;

public class IngestServiceTest {

    @Test
    void testIngestService() {
        Configs configs =
                Configs.newBuilder()
                        .put(CommonConfig.NODE_IDX.getKey(), "0")
                        .put(CommonConfig.STORE_NODE_COUNT.getKey(), "1")
                        .put(IngestorConfig.INGESTOR_CHECK_PROCESSOR_INTERVAL_MS.getKey(), "100")
                        .build();

        MockDiscovery mockDiscovery = new MockDiscovery();

        MetaService mockMetaService = mock(MetaService.class);
        when(mockMetaService.getQueueIdsForIngestor(0)).thenReturn(Arrays.asList(0));

        LogService mockLogService = mock(LogService.class);

        IngestProgressFetcher mockIngestProgressFetcher = mock(IngestProgressFetcher.class);
        when(mockIngestProgressFetcher.getTailOffsets(Arrays.asList(0)))
                .thenReturn(Arrays.asList(50L));

        StoreWriter mockStoreWriter = mock(StoreWriter.class);

        IngestService spyIngestService =
                spy(
                        new IngestService(
                                configs,
                                mockDiscovery,
                                mockMetaService,
                                mockLogService,
                                mockIngestProgressFetcher,
                                mockStoreWriter,
                                new MetricsCollector(configs)));

        IngestProcessor mockIngestProcessor = mock(IngestProcessor.class);
        doReturn(mockIngestProcessor)
                .when(spyIngestService)
                .makeIngestProcessor(any(), any(), any(), eq(0), any(), any());
        spyIngestService.start();

        verify(mockIngestProcessor, never()).start();
        mockDiscovery.addNode(RoleType.STORE, Collections.singletonMap(0, null));
        verify(mockIngestProcessor, timeout(5000L)).setTailOffset(50L);
        verify(mockIngestProcessor, timeout(5000L)).start();

        spyIngestService.advanceIngestSnapshotId(5L, null);
        verify(mockIngestProcessor)
                .ingestBatch(eq("marker"), eq(IngestService.MARKER_BATCH), any());

        mockDiscovery.removeNode(RoleType.STORE, Collections.singletonMap(0, null));
        verify(mockIngestProcessor, timeout(5000L).times(2)).stop();

        spyIngestService.stop();
    }

    class MockDiscovery implements NodeDiscovery {

        Listener listener = null;

        void addNode(RoleType roleType, Map<Integer, MaxGraphNode> nodes) {
            listener.nodesJoin(roleType, nodes);
        }

        void removeNode(RoleType roleType, Map<Integer, MaxGraphNode> nodes) {
            listener.nodesLeft(roleType, nodes);
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}

        @Override
        public void addListener(Listener listener) {
            this.listener = listener;
        }

        @Override
        public void removeListener(Listener listener) {
            this.listener = null;
        }

        @Override
        public MaxGraphNode getLocalNode() {
            return null;
        }
    }
}
