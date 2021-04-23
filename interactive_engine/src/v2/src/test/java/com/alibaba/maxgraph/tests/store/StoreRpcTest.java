package com.alibaba.maxgraph.tests.store;

import com.alibaba.maxgraph.proto.v2.CommitSnapshotIdRequest;
import com.alibaba.maxgraph.proto.v2.FetchSchemaRequest;
import com.alibaba.maxgraph.proto.v2.FetchSchemaResponse;
import com.alibaba.maxgraph.proto.v2.GraphDefPb;
import com.alibaba.maxgraph.proto.v2.SnapshotCommitGrpc;
import com.alibaba.maxgraph.proto.v2.WriteStoreRequest;
import com.alibaba.maxgraph.proto.v2.WriteStoreResponse;
import com.alibaba.maxgraph.v2.store.SnapshotCommitClient;
import com.alibaba.maxgraph.v2.store.StoreSchemaService;
import com.alibaba.maxgraph.v2.store.StoreService;
import com.alibaba.maxgraph.v2.store.StoreWriteService;
import com.alibaba.maxgraph.v2.store.WriterAgent;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.*;

public class StoreRpcTest {

    @Test
    void testSnapshotCommitClient() {
        SnapshotCommitGrpc.SnapshotCommitBlockingStub stub = mock(SnapshotCommitGrpc.SnapshotCommitBlockingStub.class);
        SnapshotCommitClient client = new SnapshotCommitClient(stub);
        client.commitSnapshotId(1, 10L, 8L, Arrays.asList(11L, 12L));
        verify(stub).commitSnapshotId(CommitSnapshotIdRequest.newBuilder()
                .setStoreId(1)
                .setSnapshotId(10L)
                .setDdlSnapshotId(8L)
                .addAllQueueOffsets(Arrays.asList(11L, 12L))
                .build());
    }

    @Test
    void testStoreSchemaService() throws IOException {
        StoreService storeService = mock(StoreService.class);
        when(storeService.getGraphDefBlob()).thenReturn(GraphDefPb.newBuilder().build());
        StoreSchemaService storeSchemaService = new StoreSchemaService(storeService);
        StreamObserver observer = mock(StreamObserver.class);
        storeSchemaService.fetchSchema(FetchSchemaRequest.newBuilder().build(), observer);
        verify(observer).onNext(FetchSchemaResponse.newBuilder().setGraphDef(GraphDefPb.newBuilder().build()).build());
        verify(observer).onCompleted();
    }

    @Test
    void testStoreWriteService() throws InterruptedException {
        WriterAgent writerAgent = mock(WriterAgent.class);
        when(writerAgent.writeStore(any())).thenReturn(true);
        StoreWriteService storeWriteService = new StoreWriteService(writerAgent);
        StreamObserver observer = mock(StreamObserver.class);
        storeWriteService.writeStore(WriteStoreRequest.newBuilder().build(), observer);
        verify(observer).onNext(WriteStoreResponse.newBuilder().setSuccess(true).build());
        verify(observer).onCompleted();
    }
}
