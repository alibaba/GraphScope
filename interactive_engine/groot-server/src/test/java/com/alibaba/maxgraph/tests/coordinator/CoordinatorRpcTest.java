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

import com.alibaba.graphscope.groot.coordinator.*;
import com.alibaba.graphscope.groot.coordinator.BackupService;
import com.alibaba.graphscope.groot.coordinator.SchemaService;
import com.alibaba.graphscope.groot.coordinator.SnapshotCommitService;
import com.alibaba.graphscope.groot.store.StoreBackupId;
import com.alibaba.maxgraph.proto.groot.*;
import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.schema.GraphDef;
import com.alibaba.graphscope.groot.schema.request.DdlException;
import com.alibaba.graphscope.groot.frontend.IngestorWriteClient;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
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

    @Test
    void testBackupService() throws IOException {
        BackupManager mockBackupManger = mock(BackupManager.class);
        BackupService backupService = new BackupService(mockBackupManger);

        when(mockBackupManger.createNewBackup()).thenReturn(6);
        StreamObserver<CreateNewBackupResponse> mockCreateObserver = mock(StreamObserver.class);
        backupService.createNewBackup(CreateNewBackupRequest.newBuilder().build(), mockCreateObserver);
        verify(mockCreateObserver).onNext(CreateNewBackupResponse.newBuilder().setGlobalBackupId(6).build());
        verify(mockCreateObserver).onCompleted();

        StreamObserver<DeleteBackupResponse> mockDeleteObserver = mock(StreamObserver.class);
        backupService.deleteBackup(DeleteBackupRequest.newBuilder().setGlobalBackupId(8).build(), mockDeleteObserver);
        verify(mockBackupManger).deleteBackup(8);
        verify(mockDeleteObserver).onNext(DeleteBackupResponse.newBuilder().build());
        verify(mockDeleteObserver).onCompleted();

        StreamObserver<PurgeOldBackupsResponse> mockPurgeObserver = mock(StreamObserver.class);
        backupService.purgeOldBackups(PurgeOldBackupsRequest.newBuilder().setKeepAliveNumber(5).build(), mockPurgeObserver);
        verify(mockBackupManger).purgeOldBackups(5);
        verify(mockPurgeObserver).onNext(PurgeOldBackupsResponse.newBuilder().build());
        verify(mockPurgeObserver).onCompleted();

        StreamObserver<RestoreFromBackupResponse> mockRestoreObserver = mock(StreamObserver.class);
        backupService.restoreFromBackup(
                RestoreFromBackupRequest.newBuilder()
                        .setGlobalBackupId(9)
                        .setMetaRestorePath("restore_meta")
                        .setStoreRestorePath("restore_store")
                        .build(),
                mockRestoreObserver);
        verify(mockBackupManger).restoreFromBackup(9, "restore_meta", "restore_store");
        verify(mockRestoreObserver).onNext(RestoreFromBackupResponse.newBuilder().build());
        verify(mockRestoreObserver).onCompleted();

        StreamObserver<VerifyBackupResponse> mockVerifyObserver = mock(StreamObserver.class);
        backupService.verifyBackup(VerifyBackupRequest.newBuilder().setGlobalBackupId(7).build(), mockVerifyObserver);
        verify(mockBackupManger).verifyBackup(7);
        verify(mockVerifyObserver).onNext(VerifyBackupResponse.newBuilder().build());
        verify(mockVerifyObserver).onCompleted();

        BackupInfo backupInfo1 = new BackupInfo(
                1, 10L, GraphDef.newBuilder().setVersion(1L).build().toProto().toByteArray(),
                new ArrayList<>(), new HashMap<>());
        BackupInfo backupInfo2 = new BackupInfo(
                2, 10L, GraphDef.newBuilder().setVersion(2L).build().toProto().toByteArray(),
                new ArrayList<>(), new HashMap<>());
        when(mockBackupManger.getBackupInfoList()).thenReturn(Arrays.asList(backupInfo1, backupInfo2));
        StreamObserver<GetBackupInfoResponse> mockGetInfoObserver = mock(StreamObserver.class);
        backupService.getBackupInfo(GetBackupInfoRequest.newBuilder().build(), mockGetInfoObserver);
        verify(mockGetInfoObserver).onNext(
                GetBackupInfoResponse.newBuilder()
                        .addBackupInfoList(backupInfo1.toProto())
                        .addBackupInfoList(backupInfo2.toProto())
                        .build());
        verify(mockGetInfoObserver).onCompleted();
    }

    @Test
    void testStoreBackupClient() {
        StoreBackupGrpc.StoreBackupStub stub = mock(StoreBackupGrpc.StoreBackupStub.class);
        StoreBackupClient client = new StoreBackupClient(stub);

        StoreBackupId storeBackupId = new StoreBackupId(3);
        storeBackupId.addPartitionBackupId(0, 3);
        storeBackupId.addPartitionBackupId(1, 3);
        Map<Integer, List<Integer>> readyPartitionBackupIds = new HashMap<>();
        readyPartitionBackupIds.put(6, new ArrayList<>());
        String storeRestoreRootPath = "store_restore_path";

        doAnswer(
                        invocation -> {
                            CreateStoreBackupRequest request = invocation.getArgument(0);
                            assertEquals(request.getGlobalBackupId(), 3);
                            StreamObserver<CreateStoreBackupResponse> observer = invocation.getArgument(1);
                            observer.onNext(
                                    CreateStoreBackupResponse.newBuilder()
                                            .setStoreBackupId(storeBackupId.toProto())
                                            .build());
                            observer.onError(null);
                            return null;
                        })
                .when(stub)
                .createStoreBackup(any(), any());
        doAnswer(
                        invocation -> {
                            ClearUnavailableStoreBackupsRequest request = invocation.getArgument(0);
                            assertEquals(request.getPartitionToReadyBackupIdsCount(), 1);
                            assertTrue(request.getPartitionToReadyBackupIdsMap().containsKey(6));
                            StreamObserver<ClearUnavailableStoreBackupsResponse> observer = invocation.getArgument(1);
                            observer.onNext(ClearUnavailableStoreBackupsResponse.newBuilder().build());
                            observer.onError(null);
                            return null;
                        })
                .when(stub)
                .clearUnavailableStoreBackups(any(), any());
        doAnswer(
                        invocation -> {
                            RestoreFromStoreBackupRequest request = invocation.getArgument(0);
                            assertEquals(StoreBackupId.parseProto(request.getStoreBackupId()), storeBackupId);
                            assertEquals(request.getRestoreRootPath(), storeRestoreRootPath);
                            StreamObserver<RestoreFromStoreBackupResponse> observer = invocation.getArgument(1);
                            observer.onNext(RestoreFromStoreBackupResponse.newBuilder().build());
                            observer.onError(null);
                            return null;
                        })
                .when(stub)
                .restoreFromStoreBackup(any(), any());
        doAnswer(
                        invocation -> {
                            VerifyStoreBackupRequest request = invocation.getArgument(0);
                            assertEquals(StoreBackupId.parseProto(request.getStoreBackupId()), storeBackupId);
                            StreamObserver<VerifyStoreBackupResponse> observer = invocation.getArgument(1);
                            observer.onNext(VerifyStoreBackupResponse.newBuilder().build());
                            observer.onError(null);
                            return null;
                        })
                .when(stub)
                .verifyStoreBackup(any(), any());

        CompletionCallback creationCallback = mock(CompletionCallback.class);
        client.createStoreBackup(3, creationCallback);
        verify(creationCallback).onCompleted(storeBackupId);
        verify(creationCallback).onError(null);
        CompletionCallback voidCallback = mock(CompletionCallback.class);
        client.clearUnavailableBackups(readyPartitionBackupIds, voidCallback);
        client.restoreFromStoreBackup(storeBackupId, storeRestoreRootPath, voidCallback);
        client.verifyStoreBackup(storeBackupId, voidCallback);
        verify(voidCallback, times(3)).onCompleted(null);
        verify(voidCallback, times(3)).onError(null);
    }
}
