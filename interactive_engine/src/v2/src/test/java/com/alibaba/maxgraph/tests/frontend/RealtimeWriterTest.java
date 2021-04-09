package com.alibaba.maxgraph.tests.frontend;

import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.OperationBlob;
import com.alibaba.maxgraph.v2.common.SnapshotListener;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.util.UuidUtils;
import com.alibaba.maxgraph.v2.frontend.IngestorWriteClient;
import com.alibaba.maxgraph.v2.frontend.RealtimeWriter;
import com.alibaba.maxgraph.v2.frontend.SnapshotCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class RealtimeWriterTest {

    @Test
    @Timeout(5)
    void testWriter() {
        MetaService mockMetaService = mock(MetaService.class);
        when(mockMetaService.getIngestorIdForQueue(1)).thenReturn(1);
        when(mockMetaService.getQueueCount()).thenReturn(2);

        SnapshotCache mockSnapshotCache = mock(SnapshotCache.class);
        doAnswer(invocationOnMock -> {
            SnapshotListener listener = invocationOnMock.getArgument(1);
            listener.onSnapshotAvailable();
            return null;
        }).when(mockSnapshotCache).addListener(anyLong(), any());

        RoleClients<IngestorWriteClient> mockClients = mock(RoleClients.class);
        IngestorWriteClient mockClient = mock(IngestorWriteClient.class);
        when(mockClients.getClient(1)).thenReturn(mockClient);

        RealtimeWriter realtimeWriter = new RealtimeWriter(mockMetaService, mockSnapshotCache, mockClients, null);
        String requestId = UuidUtils.getBase64UUIDString();
        String sessionId = "session0";
        OperationBatch operationBatch = OperationBatch.newBuilder().addOperationBlob(OperationBlob.MARKER_OPERATION_BLOB)
                .build();
        realtimeWriter.writeOperations(requestId, sessionId, operationBatch);
        verify(mockClient).writeIngestor(requestId, 1, operationBatch);

        realtimeWriter.waitForSnapshotCompletion(0L);
    }
}
