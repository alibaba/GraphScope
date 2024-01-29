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
package com.alibaba.graphscope.groot.tests.ingestor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.frontend.IngestorSnapshotService;
import com.alibaba.graphscope.groot.frontend.IngestorWriteService;
import com.alibaba.graphscope.groot.frontend.write.GraphWriter;
import com.alibaba.graphscope.groot.frontend.write.KafkaAppender;
import com.alibaba.graphscope.groot.ingestor.IngestCallback;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.proto.groot.AdvanceIngestSnapshotIdRequest;
import com.alibaba.graphscope.proto.groot.AdvanceIngestSnapshotIdResponse;
import com.alibaba.graphscope.proto.groot.WriteIngestorRequest;
import com.alibaba.graphscope.proto.groot.WriteIngestorResponse;

import io.grpc.stub.StreamObserver;

import org.junit.jupiter.api.Test;

public class IngestorRpcTest {

    @Test
    void testIngestorSnapshotService() {
        KafkaAppender kafkaAppender = mock(KafkaAppender.class);
        IngestorSnapshotService ingestorSnapshotService = new IngestorSnapshotService(kafkaAppender);
        AdvanceIngestSnapshotIdRequest req =
                AdvanceIngestSnapshotIdRequest.newBuilder().setSnapshotId(10L).build();
        StreamObserver<AdvanceIngestSnapshotIdResponse> streamObserver = mock(StreamObserver.class);
        doAnswer(
                        invocation -> {
                            CompletionCallback<Long> callback = invocation.getArgument(1);
                            callback.onCompleted(9L);
                            return null;
                        })
                .when(kafkaAppender)
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
        KafkaAppender kafkaAppender = mock(KafkaAppender.class);
        IngestorWriteService ingestorWriteService = new IngestorWriteService(kafkaAppender);
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
                .when(kafkaAppender)
                .ingestBatch(eq("test_req"), eq(OperationBatch.newBuilder().build()), any());
        StreamObserver<WriteIngestorResponse> observer = mock(StreamObserver.class);
        ingestorWriteService.writeIngestor(req, observer);
        verify(observer).onNext(WriteIngestorResponse.newBuilder().setSnapshotId(10L).build());
        verify(observer).onCompleted();
    }
}
