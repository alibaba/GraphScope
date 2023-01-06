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
package com.alibaba.graphscope.groot.frontend;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.operation.BatchId;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.proto.groot.IngestorWriteGrpc;
import com.alibaba.graphscope.proto.groot.WriteIngestorRequest;
import com.alibaba.graphscope.proto.groot.WriteIngestorResponse;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class IngestorWriteClient extends RpcClient {

    private IngestorWriteGrpc.IngestorWriteBlockingStub stub;
    private IngestorWriteGrpc.IngestorWriteStub asyncStub;

    public IngestorWriteClient(ManagedChannel channel) {
        super(channel);
        this.stub = IngestorWriteGrpc.newBlockingStub(channel);
        this.asyncStub = IngestorWriteGrpc.newStub(channel);
    }

    public IngestorWriteClient(IngestorWriteGrpc.IngestorWriteBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public BatchId writeIngestor(String requestId, int queueId, OperationBatch operationBatch) {
        WriteIngestorRequest request =
                WriteIngestorRequest.newBuilder()
                        .setRequestId(requestId)
                        .setQueueId(queueId)
                        .setOperationBatch(operationBatch.toProto())
                        .build();
        WriteIngestorResponse response = this.stub.writeIngestor(request);
        return new BatchId(response.getSnapshotId());
    }

    public void writeIngestorAsync(
            String requestId,
            int queueId,
            OperationBatch operationBatch,
            CompletionCallback<Long> callback) {
        WriteIngestorRequest request =
                WriteIngestorRequest.newBuilder()
                        .setRequestId(requestId)
                        .setQueueId(queueId)
                        .setOperationBatch(operationBatch.toProto())
                        .build();
        this.asyncStub.writeIngestor(
                request,
                new StreamObserver<WriteIngestorResponse>() {
                    @Override
                    public void onNext(WriteIngestorResponse response) {
                        callback.onCompleted(response.getSnapshotId());
                    }

                    @Override
                    public void onError(Throwable t) {
                        callback.onError(t);
                    }

                    @Override
                    public void onCompleted() {}
                });
    }
}
