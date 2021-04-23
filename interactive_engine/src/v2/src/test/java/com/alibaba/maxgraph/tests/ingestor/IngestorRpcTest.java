package com.alibaba.maxgraph.tests.ingestor;

import com.alibaba.maxgraph.proto.v2.AdvanceIngestSnapshotIdRequest;
import com.alibaba.maxgraph.proto.v2.AdvanceIngestSnapshotIdResponse;
import com.alibaba.maxgraph.proto.v2.GetTailOffsetsResponse;
import com.alibaba.maxgraph.proto.v2.IngestProgressGrpc;
import com.alibaba.maxgraph.proto.v2.StoreWriteGrpc;
import com.alibaba.maxgraph.proto.v2.WriteIngestorRequest;
import com.alibaba.maxgraph.proto.v2.WriteIngestorResponse;
import com.alibaba.maxgraph.proto.v2.WriteStoreResponse;
import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.StoreDataBatch;
import com.alibaba.maxgraph.v2.ingestor.IngestCallback;
import com.alibaba.maxgraph.v2.ingestor.IngestProgressClient;
import com.alibaba.maxgraph.v2.ingestor.IngestService;
import com.alibaba.maxgraph.v2.ingestor.IngestorSnapshotService;
import com.alibaba.maxgraph.v2.ingestor.IngestorWriteService;
import com.alibaba.maxgraph.v2.ingestor.StoreWriteClient;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class IngestorRpcTest {

    @Test
    void testIngestorSnapshotService() {
        IngestService ingestService = mock(IngestService.class);
        IngestorSnapshotService ingestorSnapshotService = new IngestorSnapshotService(ingestService);
        AdvanceIngestSnapshotIdRequest req = AdvanceIngestSnapshotIdRequest.newBuilder().setSnapshotId(10L).build();
        StreamObserver<AdvanceIngestSnapshotIdResponse> streamObserver = mock(StreamObserver.class);
        doAnswer(invocation -> {
            CompletionCallback<Long> callback = invocation.getArgument(1);
            callback.onCompleted(9L);
            return null;
        }).when(ingestService).advanceIngestSnapshotId(anyLong(), any());
        ingestorSnapshotService.advanceIngestSnapshotId(req, streamObserver);
        verify(streamObserver).onNext(AdvanceIngestSnapshotIdResponse.newBuilder().setPreviousSnapshotId(9L).build());
        verify(streamObserver).onCompleted();
    }

    @Test
    void testIngestorWriteService() {
        IngestService ingestService = mock(IngestService.class);
        IngestorWriteService ingestorWriteService = new IngestorWriteService(ingestService);
        WriteIngestorRequest req = WriteIngestorRequest.newBuilder()
                .setQueueId(2)
                .setRequestId("test_req")
                .setOperationBatch(OperationBatch.newBuilder().build().toProto())
                .build();
        doAnswer(invocation -> {
            IngestCallback callback = invocation.getArgument(3);
            callback.onSuccess(10L);
            return null;
        }).when(ingestService).ingestBatch(eq("test_req"), eq(2), eq(OperationBatch.newBuilder().build()),
                any());
        StreamObserver<WriteIngestorResponse> observer = mock(StreamObserver.class);
        ingestorWriteService.writeIngestor(req, observer);
        verify(observer).onNext(WriteIngestorResponse.newBuilder().setSnapshotId(10L).build());
        verify(observer).onCompleted();
    }

    @Test
    void testIngestProgressClient() {
        IngestProgressGrpc.IngestProgressBlockingStub stub = mock(IngestProgressGrpc.IngestProgressBlockingStub.class);
        IngestProgressClient client = new IngestProgressClient(stub);
        when(stub.getTailOffsets(any())).thenReturn(GetTailOffsetsResponse.newBuilder()
                .addAllOffsets(Arrays.asList(10L, 20L, 30L)).build());
        List<Long> tailOffsets = client.getTailOffsets(Arrays.asList(1, 2, 3));
        assertEquals(tailOffsets, Arrays.asList(10L, 20L, 30L));
    }

    @Test
    void testStoreWriteClient() {
        StoreWriteGrpc.StoreWriteStub stub = mock(StoreWriteGrpc.StoreWriteStub.class);
        StoreWriteClient client = new StoreWriteClient(stub);
        CompletionCallback callback = mock(CompletionCallback.class);
        doAnswer(invocation -> {
            StreamObserver<WriteStoreResponse> observer = invocation.getArgument(1);
            observer.onNext(WriteStoreResponse.newBuilder().setSuccess(true).build());
            return null;
        }).when(stub).writeStore(any(), any());
        client.writeStore(StoreDataBatch.newBuilder().requestId("test_req").build(), callback);
        verify(callback).onCompleted(null);
    }
}
