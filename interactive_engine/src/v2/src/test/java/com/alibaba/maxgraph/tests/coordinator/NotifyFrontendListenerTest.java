package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.coordinator.FrontendSnapshotClient;
import com.alibaba.maxgraph.v2.coordinator.NotifyFrontendListener;
import com.alibaba.maxgraph.v2.coordinator.SchemaManager;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class NotifyFrontendListenerTest {

    @Test
    void testListener() {
        FrontendSnapshotClient frontendSnapshotClient = mock(FrontendSnapshotClient.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        GraphDef graphDef = GraphDef.newBuilder().setVersion(3L).build();
        when(schemaManager.getGraphDef()).thenReturn(graphDef);
        doAnswer(invocationOnMock -> {
            long snapshotId = invocationOnMock.getArgument(0);
            CompletionCallback<Long> callback = invocationOnMock.getArgument(2);
            callback.onCompleted(snapshotId - 1);
            return null;
        }).when(frontendSnapshotClient).advanceQuerySnapshot(anyLong(), any(), any());

        NotifyFrontendListener listener = new NotifyFrontendListener(0, frontendSnapshotClient, schemaManager);
        listener.snapshotAdvanced(10L, 10L);
        verify(frontendSnapshotClient).advanceQuerySnapshot(eq(10L), eq(graphDef), any());

        listener.snapshotAdvanced(20L, 10L);
        verify(frontendSnapshotClient).advanceQuerySnapshot(eq(20L), isNull(), any());
    }
}
