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
package com.alibaba.maxgraph.tests.ingestor;

import com.alibaba.maxgraph.proto.groot.AdvanceIngestSnapshotIdRequest;
import com.alibaba.maxgraph.proto.groot.AdvanceIngestSnapshotIdResponse;
import com.alibaba.maxgraph.proto.groot.GetTailOffsetsResponse;
import com.alibaba.maxgraph.proto.groot.IngestProgressGrpc;
import com.alibaba.maxgraph.proto.groot.StoreWriteGrpc;
import com.alibaba.maxgraph.proto.groot.WriteIngestorRequest;
import com.alibaba.maxgraph.proto.groot.WriteIngestorResponse;
import com.alibaba.maxgraph.proto.groot.WriteStoreResponse;
import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.graphscope.groot.ingestor.IngestCallback;
import com.alibaba.graphscope.groot.ingestor.IngestProgressClient;
import com.alibaba.graphscope.groot.ingestor.IngestService;
import com.alibaba.graphscope.groot.ingestor.IngestorSnapshotService;
import com.alibaba.graphscope.groot.ingestor.IngestorWriteService;
import com.alibaba.graphscope.groot.ingestor.StoreWriteClient;
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
        IngestorSnapshotService ingestorSnapshotService =
                new IngestorSnapshotService(ingestService);
        AdvanceIngestSnapshotIdRequest req =
                AdvanceIngestSnapshotIdRequest.newBuilder().setSnapshotId(10L).build();
        StreamObserver<AdvanceIngestSnapshotIdResponse> streamObserver = mock(StreamObserver.class);
        doAnswer(
                        invocation -> {
                            CompletionCallback<Long> callback = invocation.getArgument(1);
                            callback.onCompleted(9L);
                            return null;
                        })
                .when(ingestService)
                .advanceIngestSnapshotId(anyLong(), any());
        ingestorSnapshotService.advanceIngestSnapshotId(req, streamObserver);
        verify(streamObserver)
                .onNext(
                        AdvanceIngestSnapshotIdResponse.newBuilder()
                                .setPreviousSnapshotId(9L)
                                .build());
        verify(streamObserver).onCompleted();
    }

    @Test
    void testIngestorWriteService() {
        IngestService ingestService = mock(IngestService.class);
        IngestorWriteService ingestorWriteService = new IngestorWriteService(ingestService);
        WriteIngestorRequest req =
                WriteIngestorRequest.newBuilder()
                        .setQueueId(2)
                        .setRequestId("test_req")
                        .setOperationBatch(OperationBatch.newBuilder().build().toProto())
                        .build();
        doAnswer(
                        invocation -> {
                            IngestCallback callback = invocation.getArgument(3);
                            callback.onSuccess(10L);
                            return null;
                        })
                .when(ingestService)
                .ingestBatch(eq("test_req"), eq(2), eq(OperationBatch.newBuilder().build()), any());
        StreamObserver<WriteIngestorResponse> observer = mock(StreamObserver.class);
        ingestorWriteService.writeIngestor(req, observer);
        verify(observer).onNext(WriteIngestorResponse.newBuilder().setSnapshotId(10L).build());
        verify(observer).onCompleted();
    }

    @Test
    void testIngestProgressClient() {
        IngestProgressGrpc.IngestProgressBlockingStub stub =
                mock(IngestProgressGrpc.IngestProgressBlockingStub.class);
        IngestProgressClient client = new IngestProgressClient(stub);
        when(stub.getTailOffsets(any()))
                .thenReturn(
                        GetTailOffsetsResponse.newBuilder()
                                .addAllOffsets(Arrays.asList(10L, 20L, 30L))
                                .build());
        List<Long> tailOffsets = client.getTailOffsets(Arrays.asList(1, 2, 3));
        assertEquals(tailOffsets, Arrays.asList(10L, 20L, 30L));
    }

    @Test
    void testStoreWriteClient() {
        StoreWriteGrpc.StoreWriteStub stub = mock(StoreWriteGrpc.StoreWriteStub.class);
        StoreWriteClient client = new StoreWriteClient(stub);
        CompletionCallback callback = mock(CompletionCallback.class);
        doAnswer(
                        invocation -> {
                            StreamObserver<WriteStoreResponse> observer = invocation.getArgument(1);
                            observer.onNext(
                                    WriteStoreResponse.newBuilder().setSuccess(true).build());
                            return null;
                        })
                .when(stub)
                .writeStore(any(), any());
        client.writeStore(StoreDataBatch.newBuilder().requestId("test_req").build(), callback);
        verify(callback).onCompleted(10);
    }
}
