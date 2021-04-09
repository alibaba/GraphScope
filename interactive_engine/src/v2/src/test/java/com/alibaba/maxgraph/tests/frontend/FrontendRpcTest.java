package com.alibaba.maxgraph.tests.frontend;

import com.alibaba.maxgraph.proto.v2.AdvanceQuerySnapshotRequest;
import com.alibaba.maxgraph.proto.v2.AdvanceQuerySnapshotResponse;
import com.alibaba.maxgraph.proto.v2.IngestorWriteGrpc;
import com.alibaba.maxgraph.proto.v2.OperationBatchPb;
import com.alibaba.maxgraph.proto.v2.SchemaGrpc;
import com.alibaba.maxgraph.proto.v2.SubmitBatchDdlResponse;
import com.alibaba.maxgraph.proto.v2.WriteIngestorRequest;
import com.alibaba.maxgraph.proto.v2.WriteIngestorResponse;
import com.alibaba.maxgraph.v2.common.BatchId;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.SnapshotWithSchema;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;
import com.alibaba.maxgraph.v2.common.schema.request.DdlRequestBatch;
import com.alibaba.maxgraph.v2.coordinator.GraphDefFetcher;
import com.alibaba.maxgraph.v2.coordinator.StoreSchemaClient;
import com.alibaba.maxgraph.v2.frontend.FrontendSnapshotService;
import com.alibaba.maxgraph.v2.frontend.IngestorWriteClient;
import com.alibaba.maxgraph.v2.frontend.SchemaClient;
import com.alibaba.maxgraph.v2.frontend.SchemaWriter;
import com.alibaba.maxgraph.v2.frontend.SnapshotCache;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class FrontendRpcTest {

    @Test
    void testFrontendSnapshotService() {
        SnapshotCache snapshotCache = new SnapshotCache();
        FrontendSnapshotService frontendSnapshotService = new FrontendSnapshotService(snapshotCache);
        AdvanceQuerySnapshotRequest req = AdvanceQuerySnapshotRequest.newBuilder()
                .setGraphDef(GraphDef.newBuilder().setVersion(5L).build().toProto())
                .setSnapshotId(10L)
                .build();
        StreamObserver<AdvanceQuerySnapshotResponse> streamObserver = mock(StreamObserver.class);
        frontendSnapshotService.advanceQuerySnapshot(req, streamObserver);
        SnapshotWithSchema snapshotWithSchema = snapshotCache.getSnapshotWithSchema();
        assertEquals(snapshotWithSchema.getSnapshotId(), 10L);
        assertEquals(snapshotWithSchema.getGraphDef().getVersion(), 5L);
        verify(streamObserver).onNext(AdvanceQuerySnapshotResponse.newBuilder().setPreviousSnapshotId(-1L).build());
        verify(streamObserver).onCompleted();
    }

    @Test
    void testIngestorWriteClient() {
        IngestorWriteGrpc.IngestorWriteBlockingStub stub = mock(IngestorWriteGrpc.IngestorWriteBlockingStub.class);
        IngestorWriteClient client = new IngestorWriteClient(stub);
        when(stub.writeIngestor(any())).thenReturn(WriteIngestorResponse.newBuilder().setSnapshotId(10L).build());
        BatchId batchId = client.writeIngestor("test_req", 2, OperationBatch.newBuilder().build());
        assertEquals(batchId.getSnapshotId(), 10L);
        verify(stub).writeIngestor(WriteIngestorRequest.newBuilder()
                .setRequestId("test_req")
                .setQueueId(2)
                .setOperationBatch(OperationBatch.newBuilder().build().toProto())
                .build());
    }

    @Test
    void testSchemaClient() {
        SchemaGrpc.SchemaBlockingStub stub = mock(SchemaGrpc.SchemaBlockingStub.class);
        when(stub.submitBatchDdl(any())).thenReturn(SubmitBatchDdlResponse.newBuilder()
                .setSuccess(true)
                .setDdlSnapshotId(10L).build());
        SchemaClient schemaClient = new SchemaClient(stub);
        long snapshotId = schemaClient.submitBatchDdl("test_req", "test_session",
                DdlRequestBatch.newBuilder().build());
        assertEquals(snapshotId, 10L);
    }

    @Test
    void testSchemaWriter() {
        RoleClients<SchemaClient> clients = mock(RoleClients.class);
        SchemaClient client = mock(SchemaClient.class);
        when(clients.getClient(0)).thenReturn(client);

        SchemaWriter schemaWriter = new SchemaWriter(clients);
        schemaWriter.submitBatchDdl("test_req", "test_session", DdlRequestBatch.newBuilder().build());
        verify(client).submitBatchDdl("test_req", "test_session",
                DdlRequestBatch.newBuilder().build());
    }

}
