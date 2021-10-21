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

import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.graphscope.groot.wal.LogReader;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.wal.LogWriter;
import com.alibaba.graphscope.groot.wal.ReadLogEntry;
import com.alibaba.graphscope.groot.ingestor.BatchSender;
import com.alibaba.graphscope.groot.ingestor.IngestCallback;
import com.alibaba.graphscope.groot.ingestor.IngestProcessor;
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

        IngestProcessor ingestProcessor =
                new IngestProcessor(
                        configs,
                        mockLogService,
                        mockBatchSender,
                        queueId,
                        ingestSnapshotId,
                        new MetricsCollector(configs));
        ingestProcessor.setTailOffset(tailOffset);
        ingestProcessor.start();

        verify(mockBatchSender, timeout(5000L))
                .asyncSendWithRetry(
                        eq(""), eq(queueId), eq(readSnapshotId), eq(tailOffset + 1), any());
        verify(mockBatchSender, timeout(5000L))
                .asyncSendWithRetry(
                        eq(""), eq(queueId), eq(readSnapshotId), eq(tailOffset + 2), any());
        verify(mockLogReader).close();

        String requestId = "test_ingest_processor";
        IngestCallback mockIngestCallback = mock(IngestCallback.class);
        ingestProcessor.ingestBatch(requestId, emptyBatch, mockIngestCallback);

        verify(mockBatchSender, timeout(5000L))
                .asyncSendWithRetry(
                        requestId, queueId, ingestSnapshotId.get(), tailOffset + 3, emptyBatch);
        verify(mockIngestCallback, timeout(5000L)).onSuccess(ingestSnapshotId.get());

        ingestProcessor.stop();
        verify(mockLogWriter, timeout(5000L)).close();
    }
}
