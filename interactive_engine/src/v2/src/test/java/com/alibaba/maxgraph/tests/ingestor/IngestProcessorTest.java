package com.alibaba.maxgraph.tests.ingestor;

import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.wal.LogReader;
import com.alibaba.maxgraph.v2.common.wal.LogService;
import com.alibaba.maxgraph.v2.common.wal.LogWriter;
import com.alibaba.maxgraph.v2.common.wal.ReadLogEntry;
import com.alibaba.maxgraph.v2.ingestor.BatchSender;
import com.alibaba.maxgraph.v2.ingestor.IngestCallback;
import com.alibaba.maxgraph.v2.ingestor.IngestProcessor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.*;

public class IngestProcessorTest {

    @Test
    void testIngestProcessor() throws IOException {
        long tailOffset = 50L;
        int queueId = 0;

        Configs configs = Configs.newBuilder().build();
        LogService mockLogService = mock(LogService.class);

        LogReader mockLogReader = mock(LogReader.class);
        when(mockLogService.createReader(queueId, tailOffset + 1)).thenReturn(mockLogReader);

        LogWriter mockLogWriter = mock(LogWriter.class);
        when(mockLogService.createWriter(queueId)).thenReturn(mockLogWriter);
        when(mockLogWriter.append(any())).thenReturn(tailOffset + 3);

        OperationBatch emptyBatch = OperationBatch.newBuilder().build();
        long readSnapshotId = 5L;
        ReadLogEntry readLogEntry1 = new ReadLogEntry(tailOffset + 1, readSnapshotId, emptyBatch);
        ReadLogEntry readLogEntry2 = new ReadLogEntry(tailOffset + 2, readSnapshotId, emptyBatch);
        when(mockLogReader.readNext())
                .thenReturn(readLogEntry1)
                .thenReturn(readLogEntry2)
                .thenReturn(null);

        BatchSender mockBatchSender = mock(BatchSender.class);
        AtomicLong ingestSnapshotId = new AtomicLong(10L);

        IngestProcessor ingestProcessor = new IngestProcessor(configs, mockLogService, mockBatchSender, queueId,
                ingestSnapshotId, null);
        ingestProcessor.setTailOffset(tailOffset);
        ingestProcessor.start();

        verify(mockBatchSender, timeout(5000L)).asyncSendWithRetry(eq(""), eq(queueId), eq(readSnapshotId),
                eq(tailOffset + 1), any());
        verify(mockBatchSender, timeout(5000L)).asyncSendWithRetry(eq(""), eq(queueId), eq(readSnapshotId),
                eq(tailOffset + 2), any());
        verify(mockLogReader).close();

        String requestId = "test_ingest_processor";
        IngestCallback mockIngestCallback = mock(IngestCallback.class);
        ingestProcessor.ingestBatch(requestId, emptyBatch, mockIngestCallback);

        verify(mockBatchSender, timeout(5000L)).asyncSendWithRetry(requestId, queueId, ingestSnapshotId.get(),
                tailOffset + 3, emptyBatch);
        verify(mockIngestCallback, timeout(5000L)).onSuccess(ingestSnapshotId.get());

        ingestProcessor.stop();
        verify(mockLogWriter, timeout(5000L)).close();
    }
}
