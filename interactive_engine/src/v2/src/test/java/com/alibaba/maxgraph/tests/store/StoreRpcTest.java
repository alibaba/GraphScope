/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.tests.store;

import com.alibaba.maxgraph.proto.v2.CommitSnapshotIdRequest;
import com.alibaba.maxgraph.proto.v2.FetchSchemaRequest;
import com.alibaba.maxgraph.proto.v2.FetchSchemaResponse;
import com.alibaba.maxgraph.proto.v2.GraphDefPb;
import com.alibaba.maxgraph.proto.v2.SnapshotCommitGrpc;
import com.alibaba.maxgraph.proto.v2.WriteStoreRequest;
import com.alibaba.maxgraph.proto.v2.WriteStoreResponse;
import com.alibaba.maxgraph.groot.store.SnapshotCommitClient;
import com.alibaba.maxgraph.groot.store.StoreSchemaService;
import com.alibaba.maxgraph.groot.store.StoreService;
import com.alibaba.maxgraph.groot.store.StoreWriteService;
import com.alibaba.maxgraph.groot.store.WriterAgent;
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
