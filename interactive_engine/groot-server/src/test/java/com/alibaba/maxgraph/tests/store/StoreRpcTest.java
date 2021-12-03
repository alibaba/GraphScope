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
package com.alibaba.maxgraph.tests.store;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.store.*;
import com.alibaba.graphscope.groot.store.StoreBackupService;
import com.alibaba.graphscope.groot.store.StoreSchemaService;
import com.alibaba.graphscope.groot.store.StoreWriteService;
import com.alibaba.maxgraph.proto.groot.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.*;

public class StoreRpcTest {

    @Test
    void testSnapshotCommitClient() {
        SnapshotCommitGrpc.SnapshotCommitBlockingStub stub =
                mock(SnapshotCommitGrpc.SnapshotCommitBlockingStub.class);
        SnapshotCommitClient client = new SnapshotCommitClient(stub);
        client.commitSnapshotId(1, 10L, 8L, Arrays.asList(11L, 12L));
        verify(stub)
                .commitSnapshotId(
                        CommitSnapshotIdRequest.newBuilder()
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
        verify(observer)
                .onNext(
                        FetchSchemaResponse.newBuilder()
                                .setGraphDef(GraphDefPb.newBuilder().build())
                                .build());
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

    @Test
    void testStoreBackupService() {
        BackupAgent mockBackupAgent = mock(BackupAgent.class);
        StoreBackupService storeBackupService = new StoreBackupService(mockBackupAgent);

        StoreBackupId storeBackupId = new StoreBackupId(6);
        storeBackupId.addPartitionBackupId(0, 6);
        storeBackupId.addPartitionBackupId(1, 6);

        CreateStoreBackupRequest createRequest = CreateStoreBackupRequest.newBuilder()
                .setGlobalBackupId(6)
                .build();
        doAnswer(
                        invocation -> {
                            CompletionCallback<StoreBackupId> callback = invocation.getArgument(1);
                            callback.onCompleted(storeBackupId);
                            return null;
                        })
                .when(mockBackupAgent)
                .createNewStoreBackup(eq(6), any());
        StreamObserver<CreateStoreBackupResponse> mockCreateObserver = mock(StreamObserver.class);
        storeBackupService.createStoreBackup(createRequest, mockCreateObserver);
        verify(mockCreateObserver).onNext(
                CreateStoreBackupResponse.newBuilder()
                        .setStoreBackupId(storeBackupId.toProto())
                        .build());
        verify(mockCreateObserver).onCompleted();

        VerifyStoreBackupRequest verifyRequest = VerifyStoreBackupRequest.newBuilder()
                .setStoreBackupId(storeBackupId.toProto())
                .build();
        doAnswer(
                        invocation -> {
                            CompletionCallback<Void> callback = invocation.getArgument(1);
                            callback.onCompleted(null);
                            return null;
                        })
                .when(mockBackupAgent)
                .verifyStoreBackup(eq(storeBackupId), any());
        StreamObserver<VerifyStoreBackupResponse> mockVerifyObserver = mock(StreamObserver.class);
        storeBackupService.verifyStoreBackup(verifyRequest, mockVerifyObserver);
        verify(mockVerifyObserver).onNext(VerifyStoreBackupResponse.newBuilder().build());
        verify(mockVerifyObserver).onCompleted();

        ClearUnavailableStoreBackupsRequest clearRequest = ClearUnavailableStoreBackupsRequest.newBuilder()
                .putPartitionToReadyBackupIds(0, PartitionBackupIdListPb.newBuilder().build())
                .putPartitionToReadyBackupIds(1, PartitionBackupIdListPb.newBuilder().build())
                .build();
        Map<Integer, List<Integer>> readyPartitionBackupIds = new HashMap<>();
        readyPartitionBackupIds.put(0, new ArrayList<>());
        readyPartitionBackupIds.put(1, new ArrayList<>());
        doAnswer(
                        invocation -> {
                            CompletionCallback<Void> callback = invocation.getArgument(1);
                            callback.onCompleted(null);
                            return null;
                        })
                .when(mockBackupAgent)
                .clearUnavailableStoreBackups(eq(readyPartitionBackupIds), any());
        StreamObserver<ClearUnavailableStoreBackupsResponse> mockClearObserver = mock(StreamObserver.class);
        storeBackupService.clearUnavailableStoreBackups(clearRequest, mockClearObserver);
        verify(mockClearObserver).onNext(ClearUnavailableStoreBackupsResponse.newBuilder().build());
        verify(mockClearObserver).onCompleted();

        RestoreFromStoreBackupRequest restoreRequest = RestoreFromStoreBackupRequest.newBuilder()
                .setStoreBackupId(storeBackupId.toProto())
                .setRestoreRootPath("store_restore_path")
                .build();
        doAnswer(
                        invocation -> {
                            CompletionCallback<Void> callback = invocation.getArgument(2);
                            callback.onCompleted(null);
                            return null;
                        })
                .when(mockBackupAgent)
                .restoreFromStoreBackup(eq(storeBackupId), eq("store_restore_path"), any());
        StreamObserver<RestoreFromStoreBackupResponse> mockRestoreObserver = mock(StreamObserver.class);
        storeBackupService.restoreFromStoreBackup(restoreRequest, mockRestoreObserver);
        verify(mockRestoreObserver).onNext(RestoreFromStoreBackupResponse.newBuilder().build());
        verify(mockRestoreObserver).onCompleted();
    }
}
