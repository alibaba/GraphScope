package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.maxgraph.proto.v2.GetTailOffsetsRequest;
import com.alibaba.maxgraph.proto.v2.GetTailOffsetsResponse;
import com.alibaba.maxgraph.v2.coordinator.IngestProgressService;
import com.alibaba.maxgraph.v2.coordinator.SnapshotManager;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class IngestProgressServiceTest {
    @Test
    void testIngestProgressService() {
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(snapshotManager.getTailOffsets(Arrays.asList(1))).thenReturn(Arrays.asList(10L));
        IngestProgressService ingestProgressService = new IngestProgressService(snapshotManager);
        GetTailOffsetsRequest request = GetTailOffsetsRequest.newBuilder().addQueueId(1).build();

        ingestProgressService.getTailOffsets(request, new StreamObserver<GetTailOffsetsResponse>() {
            @Override
            public void onNext(GetTailOffsetsResponse response) {
                List<Long> offsetsList = response.getOffsetsList();
                assertEquals(offsetsList.size(), 1);
                assertEquals(offsetsList.get(0), 10L);
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException(t);
            }

            @Override
            public void onCompleted() {

            }
        });
        verify(snapshotManager).getTailOffsets(Arrays.asList(1));
    }
}
