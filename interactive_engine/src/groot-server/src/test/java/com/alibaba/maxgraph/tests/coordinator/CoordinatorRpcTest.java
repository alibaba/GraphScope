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
package com.alibaba.maxgraph.tests.coordinator;

import com.alibaba.maxgraph.proto.groot.AdvanceIngestSnapshotIdRequest;
import com.alibaba.maxgraph.proto.groot.AdvanceIngestSnapshotIdResponse;
import com.alibaba.maxgraph.proto.groot.AdvanceQuerySnapshotRequest;
import com.alibaba.maxgraph.proto.groot.AdvanceQuerySnapshotResponse;
import com.alibaba.maxgraph.proto.groot.CommitSnapshotIdRequest;
import com.alibaba.maxgraph.proto.groot.CommitSnapshotIdResponse;
import com.alibaba.maxgraph.proto.groot.FetchSchemaResponse;
import com.alibaba.maxgraph.proto.groot.FrontendSnapshotGrpc;
import com.alibaba.maxgraph.proto.groot.IngestorSnapshotGrpc;
import com.alibaba.maxgraph.proto.groot.StoreSchemaGrpc;
import com.alibaba.maxgraph.proto.groot.SubmitBatchDdlRequest;
import com.alibaba.maxgraph.proto.groot.SubmitBatchDdlResponse;
import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.schema.GraphDef;
import com.alibaba.graphscope.groot.schema.request.DdlException;
import com.alibaba.graphscope.groot.coordinator.DdlWriter;
import com.alibaba.graphscope.groot.coordinator.FrontendSnapshotClient;
import com.alibaba.graphscope.groot.coordinator.GraphDefFetcher;
import com.alibaba.graphscope.groot.coordinator.IngestorSnapshotClient;
import com.alibaba.graphscope.groot.coordinator.SchemaManager;
import com.alibaba.graphscope.groot.coordinator.SchemaService;
import com.alibaba.graphscope.groot.coordinator.SnapshotCommitService;
import com.alibaba.graphscope.groot.coordinator.SnapshotManager;
import com.alibaba.graphscope.groot.coordinator.StoreSchemaClient;
import com.alibaba.graphscope.groot.frontend.IngestorWriteClient;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class CoordinatorRpcTest {

    @Test
    void testDdlWriter() {
        RoleClients<IngestorWriteClient> clients = mock(RoleClients.class);
        IngestorWriteClient ingestorWriteClient = mock(IngestorWriteClient.class);
        when(clients.getClient(0)).thenReturn(ingestorWriteClient);

        DdlWriter ddlWriter = new DdlWriter(clients);
        ddlWriter.writeOperations("test_req", null);
        verify(ingestorWriteClient).writeIngestor(eq("test_req"), eq(0), any());
    }

    @Test
    void testGraphDefFetcher() {
        RoleClients<StoreSchemaClient> clients = mock(RoleClients.class);
        StoreSchemaClient client = mock(StoreSchemaClient.class);
        when(clients.getClient(0)).thenReturn(client);

        GraphDefFetcher graphDefFetcher = new GraphDefFetcher(clients);
        graphDefFetcher.fetchGraphDef();
        verify(client).fetchSchema();
    }

    @Test
    void testStoreSchemaClient() {
        StoreSchemaGrpc.StoreSchemaBlockingStub stub =
                mock(StoreSchemaGrpc.StoreSchemaBlockingStub.class);
        when(stub.fetchSchema(any()))
                .thenReturn(
                        FetchSchemaResponse.newBuilder()
                                .setGraphDef(GraphDef.newBuilder().build().toProto())
                                .build());
        StoreSchemaClient client = new StoreSchemaClient(stub);
        assertEquals(client.fetchSchema(), GraphDef.newBuilder().build());
    }

    @Test
    void testFrontendSnapshotClient() {
        FrontendSnapshotGrpc.FrontendSnapshotStub stub =
                mock(FrontendSnapshotGrpc.FrontendSnapshotStub.class);
        FrontendSnapshotClient client = new FrontendSnapshotClient(stub);
        GraphDef graphDef = GraphDef.newBuilder().build();
        doAnswer(
                        invocationOnMock -> {
                            AdvanceQuerySnapshotRequest req = invocationOnMock.getArgument(0);
                            StreamObserver<AdvanceQuerySnapshotResponse> observer =
                                    invocationOnMock.getArgument(1);
                            assertEquals(req.getGraphDef(), graphDef.toProto());
                            assertEquals(req.getSnapshotId(), 10L);
                            observer.onNext(
                                    AdvanceQuerySnapshotResponse.newBuilder()
                                            .setPreviousSnapshotId(9L)
                                            .build());
                            return null;
                        })
                .when(stub)
                .advanceQuerySnapshot(any(), any());

        CompletionCallback completionCallback = mock(CompletionCallback.class);
        client.advanceQuerySnapshot(10L, graphDef, completionCallback);
        verify(completionCallback).onCompleted(9L);
    }

    @Test
    void testIngestorSnapshotClient() {
        IngestorSnapshotGrpc.IngestorSnapshotStub stub =
                mock(IngestorSnapshotGrpc.IngestorSnapshotStub.class);
        IngestorSnapshotClient client = new IngestorSnapshotClient(stub);
        CompletionCallback callback = mock(CompletionCallback.class);
        doAnswer(
                        invocation -> {
                            AdvanceIngestSnapshotIdRequest req = invocation.getArgument(0);
                            assertEquals(req.getSnapshotId(), 10L);
                            StreamObserver<AdvanceIngestSnapshotIdResponse> observer =
                                    invocation.getArgument(1);
                            observer.onNext(
                                    AdvanceIngestSnapshotIdResponse.newBuilder()
                                            .setPreviousSnapshotId(8L)
                                            .build());
                            observer.onError(null);
                            return null;
                        })
                .when(stub)
                .advanceIngestSnapshotId(any(), any());
        client.advanceIngestSnapshotId(10L, callback);
        verify(callback).onCompleted(8L);
        verify(callback).onError(null);
    }

    @Test
    void testSchemaService() {
        SchemaManager schemaManager = mock(SchemaManager.class);
        doAnswer(
                        invocationOnMock -> {
                            CompletionCallback<Long> callback = invocationOnMock.getArgument(3);
                            callback.onCompleted(10L);
                            callback.onError(new DdlException("test_exception"));
                            return null;
                        })
                .when(schemaManager)
                .submitBatchDdl(eq("test_req"), eq("test_session"), any(), any());

        SchemaService schemaService = new SchemaService(schemaManager);
        StreamObserver<SubmitBatchDdlResponse> streamObserver = mock(StreamObserver.class);
        schemaService.submitBatchDdl(
                SubmitBatchDdlRequest.newBuilder()
                        .setRequestId("test_req")
                        .setSessionId("test_session")
                        .build(),
                streamObserver);
        verify(streamObserver)
                .onNext(
                        SubmitBatchDdlResponse.newBuilder()
                                .setSuccess(true)
                                .setDdlSnapshotId(10L)
                                .build());
        verify(streamObserver)
                .onNext(
                        SubmitBatchDdlResponse.newBuilder()
                                .setSuccess(false)
                                .setMsg("test_exception")
                                .build());
        verify(streamObserver, times(2)).onCompleted();
    }

    @Test
    void testSnapshotCommitService() {
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        SnapshotCommitService snapshotCommitService = new SnapshotCommitService(snapshotManager);
        StreamObserver<CommitSnapshotIdResponse> streamObserver = mock(StreamObserver.class);
        snapshotCommitService.commitSnapshotId(
                CommitSnapshotIdRequest.newBuilder()
                        .setStoreId(10)
                        .setSnapshotId(20L)
                        .setDdlSnapshotId(15L)
                        .addAllQueueOffsets(Arrays.asList(1L, 2L, 3L))
                        .build(),
                streamObserver);
        verify(snapshotManager).commitSnapshotId(10, 20, 15, Arrays.asList(1L, 2L, 3L));
        verify(streamObserver).onNext(CommitSnapshotIdResponse.newBuilder().build());
        verify(streamObserver).onCompleted();
    }
}
