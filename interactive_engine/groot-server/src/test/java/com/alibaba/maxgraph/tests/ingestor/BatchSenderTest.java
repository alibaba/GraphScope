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

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.graphscope.groot.operation.LabelId;
import com.alibaba.graphscope.groot.operation.dml.OverwriteVertexOperation;
import com.alibaba.graphscope.groot.operation.VertexId;
import com.alibaba.graphscope.groot.ingestor.BatchSender;
import com.alibaba.graphscope.groot.ingestor.StoreWriter;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class BatchSenderTest {

    @Test
    void testSend() throws InterruptedException {
        Configs configs =
                Configs.newBuilder().put(CommonConfig.STORE_NODE_COUNT.getKey(), "1").build();
        MetaService mockMetaService = mock(MetaService.class);
        when(mockMetaService.getPartitionCount()).thenReturn(2);
        when(mockMetaService.getStoreIdByPartition(anyInt())).thenReturn(0);

        StoreWriter mockStoreWriter = mock(StoreWriter.class);
        String requestId = "test_batch_sender";
        int queueId = 0;
        long snapshotId = 10L;
        long offset = 50L;
        LabelId labelId = new LabelId(0);
        OperationBlob writeVertexBlob1 =
                new OverwriteVertexOperation(new VertexId(0L), labelId, Collections.EMPTY_MAP)
                        .toBlob();
        OperationBlob writeVertexBlob2 =
                new OverwriteVertexOperation(new VertexId(1L), labelId, Collections.EMPTY_MAP)
                        .toBlob();
        OperationBatch batch =
                OperationBatch.newBuilder()
                        .addOperationBlob(writeVertexBlob1)
                        .addOperationBlob(writeVertexBlob2)
                        .addOperationBlob(OperationBlob.MARKER_OPERATION_BLOB)
                        .build();

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(
                        invocationOnMock -> {
                            StoreDataBatch storeBatch = invocationOnMock.getArgument(1);
                            CompletionCallback callback = invocationOnMock.getArgument(2);
                            assertAll(
                                    () -> assertEquals(storeBatch.getRequestId(), requestId),
                                    () -> assertEquals(storeBatch.getQueueId(), queueId),
                                    () -> assertEquals(storeBatch.getSnapshotId(), snapshotId),
                                    () -> assertEquals(storeBatch.getOffset(), offset),
                                    () -> assertEquals(storeBatch.getDataBatch().size(), 2));
                            List<Map<Integer, OperationBatch>> dataBatch =
                                    storeBatch.getDataBatch();
                            Map<Integer, OperationBatch> partitionToBatch = dataBatch.get(0);
                            assertAll(
                                    () ->
                                            assertEquals(
                                                    partitionToBatch.get(0).getOperationBlob(0),
                                                    writeVertexBlob1),
                                    () ->
                                            assertEquals(
                                                    partitionToBatch.get(1).getOperationBlob(0),
                                                    writeVertexBlob2));
                            assertEquals(
                                    dataBatch.get(1).get(-1).getOperationBlob(0),
                                    OperationBlob.MARKER_OPERATION_BLOB);
                            callback.onCompleted(0);
                            latch.countDown();
                            return null;
                        })
                .when(mockStoreWriter)
                .write(anyInt(), any(), any());

        BatchSender batchSender =
                new BatchSender(
                        configs, mockMetaService, mockStoreWriter, new MetricsCollector(configs));
        batchSender.start();

        batchSender.asyncSendWithRetry(requestId, queueId, snapshotId, offset, batch);
        assertTrue(latch.await(5L, TimeUnit.SECONDS));

        batchSender.stop();
    }
}
