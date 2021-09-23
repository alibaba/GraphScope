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
package com.alibaba.maxgraph.tests.frontend;

import com.alibaba.maxgraph.proto.groot.AdvanceQuerySnapshotRequest;
import com.alibaba.maxgraph.proto.groot.AdvanceQuerySnapshotResponse;
import com.alibaba.maxgraph.proto.groot.IngestorWriteGrpc;
import com.alibaba.maxgraph.proto.groot.SchemaGrpc;
import com.alibaba.maxgraph.proto.groot.SubmitBatchDdlResponse;
import com.alibaba.maxgraph.proto.groot.WriteIngestorRequest;
import com.alibaba.maxgraph.proto.groot.WriteIngestorResponse;
import com.alibaba.graphscope.groot.operation.BatchId;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.SnapshotWithSchema;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.schema.GraphDef;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBatch;
import com.alibaba.graphscope.groot.frontend.FrontendSnapshotService;
import com.alibaba.graphscope.groot.frontend.IngestorWriteClient;
import com.alibaba.graphscope.groot.frontend.SchemaClient;
import com.alibaba.graphscope.groot.frontend.SchemaWriter;
import com.alibaba.graphscope.groot.frontend.SnapshotCache;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class FrontendRpcTest {

    @Test
    void testFrontendSnapshotService() {
        SnapshotCache snapshotCache = new SnapshotCache();
        FrontendSnapshotService frontendSnapshotService =
                new FrontendSnapshotService(snapshotCache);
        AdvanceQuerySnapshotRequest req =
                AdvanceQuerySnapshotRequest.newBuilder()
                        .setGraphDef(GraphDef.newBuilder().setVersion(5L).build().toProto())
                        .setSnapshotId(10L)
                        .build();
        StreamObserver<AdvanceQuerySnapshotResponse> streamObserver = mock(StreamObserver.class);
        frontendSnapshotService.advanceQuerySnapshot(req, streamObserver);
        SnapshotWithSchema snapshotWithSchema = snapshotCache.getSnapshotWithSchema();
        assertEquals(snapshotWithSchema.getSnapshotId(), 10L);
        assertEquals(snapshotWithSchema.getGraphDef().getVersion(), 5L);
        verify(streamObserver)
                .onNext(
                        AdvanceQuerySnapshotResponse.newBuilder()
                                .setPreviousSnapshotId(-1L)
                                .build());
        verify(streamObserver).onCompleted();
    }

    @Test
    void testIngestorWriteClient() {
        IngestorWriteGrpc.IngestorWriteBlockingStub stub =
                mock(IngestorWriteGrpc.IngestorWriteBlockingStub.class);
        IngestorWriteClient client = new IngestorWriteClient(stub);
        when(stub.writeIngestor(any()))
                .thenReturn(WriteIngestorResponse.newBuilder().setSnapshotId(10L).build());
        BatchId batchId = client.writeIngestor("test_req", 2, OperationBatch.newBuilder().build());
        assertEquals(batchId.getSnapshotId(), 10L);
        verify(stub)
                .writeIngestor(
                        WriteIngestorRequest.newBuilder()
                                .setRequestId("test_req")
                                .setQueueId(2)
                                .setOperationBatch(OperationBatch.newBuilder().build().toProto())
                                .build());
    }

    @Test
    void testSchemaClient() {
        SchemaGrpc.SchemaBlockingStub stub = mock(SchemaGrpc.SchemaBlockingStub.class);
        when(stub.submitBatchDdl(any()))
                .thenReturn(
                        SubmitBatchDdlResponse.newBuilder()
                                .setSuccess(true)
                                .setDdlSnapshotId(10L)
                                .build());
        SchemaClient schemaClient = new SchemaClient(stub);
        long snapshotId =
                schemaClient.submitBatchDdl(
                        "test_req", "test_session", DdlRequestBatch.newBuilder().build().toProto());
        assertEquals(snapshotId, 10L);
    }

    @Test
    void testSchemaWriter() {
        RoleClients<SchemaClient> clients = mock(RoleClients.class);
        SchemaClient client = mock(SchemaClient.class);
        when(clients.getClient(0)).thenReturn(client);

        SchemaWriter schemaWriter = new SchemaWriter(clients);
        schemaWriter.submitBatchDdl(
                "test_req", "test_session", DdlRequestBatch.newBuilder().build());
        verify(client)
                .submitBatchDdl(
                        "test_req", "test_session", DdlRequestBatch.newBuilder().build().toProto());
    }
}
