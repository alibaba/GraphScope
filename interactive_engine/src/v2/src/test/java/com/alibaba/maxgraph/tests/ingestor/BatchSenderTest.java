package com.alibaba.maxgraph.tests.ingestor;

import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.OperationBlob;
import com.alibaba.maxgraph.v2.common.StoreDataBatch;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.operation.LabelId;
import com.alibaba.maxgraph.v2.common.operation.dml.OverwriteVertexOperation;
import com.alibaba.maxgraph.v2.common.operation.VertexId;
import com.alibaba.maxgraph.v2.ingestor.BatchSender;
import com.alibaba.maxgraph.v2.ingestor.StoreWriter;
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
        Configs configs = Configs.newBuilder()
                .put(CommonConfig.STORE_NODE_COUNT.getKey(), "1")
                .build();
        MetaService mockMetaService = mock(MetaService.class);
        when(mockMetaService.getPartitionCount()).thenReturn(2);
        when(mockMetaService.getStoreIdByPartition(anyInt())).thenReturn(0);

        StoreWriter mockStoreWriter = mock(StoreWriter.class);
        String requestId = "test_batch_sender";
        int queueId = 0;
        long snapshotId = 10L;
        long offset = 50L;
        LabelId labelId = new LabelId(0);
        OperationBlob writeVertexBlob1 = new OverwriteVertexOperation(new VertexId(0L), labelId, Collections.EMPTY_MAP).toBlob();
        OperationBlob writeVertexBlob2 = new OverwriteVertexOperation(new VertexId(1L), labelId, Collections.EMPTY_MAP).toBlob();
        OperationBatch batch = OperationBatch.newBuilder()
                .addOperationBlob(writeVertexBlob1)
                .addOperationBlob(writeVertexBlob2)
                .addOperationBlob(OperationBlob.MARKER_OPERATION_BLOB)
                .build();

        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            StoreDataBatch storeBatch = invocationOnMock.getArgument(1);
            CompletionCallback callback = invocationOnMock.getArgument(2);
            assertAll(
                    () -> assertEquals(storeBatch.getRequestId(), requestId),
                    () -> assertEquals(storeBatch.getQueueId(), queueId),
                    () -> assertEquals(storeBatch.getSnapshotId(), snapshotId),
                    () -> assertEquals(storeBatch.getOffset(), offset),
                    () -> assertEquals(storeBatch.getDataBatch().size(), 2)
            );
            List<Map<Integer, OperationBatch>> dataBatch = storeBatch.getDataBatch();
            Map<Integer, OperationBatch> partitionToBatch = dataBatch.get(0);
            assertAll(
                    () -> assertEquals(partitionToBatch.get(0).getOperationBlob(0), writeVertexBlob1),
                    () -> assertEquals(partitionToBatch.get(1).getOperationBlob(0), writeVertexBlob2)
            );
            assertEquals(dataBatch.get(1).get(-1).getOperationBlob(0), OperationBlob.MARKER_OPERATION_BLOB);
            callback.onCompleted(null);
            latch.countDown();
            return null;
        }).when(mockStoreWriter).write(anyInt(), any(), any());

        BatchSender batchSender = new BatchSender(configs, mockMetaService, mockStoreWriter, null);
        batchSender.start();

        batchSender.asyncSendWithRetry(requestId, queueId, snapshotId, offset, batch);
        assertTrue(latch.await(5L, TimeUnit.SECONDS));

        batchSender.stop();
    }
}
