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
package com.alibaba.maxgraph.groot.frontend;

import com.alibaba.maxgraph.proto.v2.IngestorWriteGrpc;
import com.alibaba.maxgraph.proto.v2.WriteIngestorRequest;
import com.alibaba.maxgraph.proto.v2.WriteIngestorResponse;
import com.alibaba.maxgraph.groot.common.BatchId;
import com.alibaba.maxgraph.groot.common.OperationBatch;
import com.alibaba.maxgraph.groot.common.rpc.RpcClient;
import io.grpc.ManagedChannel;

public class IngestorWriteClient extends RpcClient {

    private IngestorWriteGrpc.IngestorWriteBlockingStub stub;

    public IngestorWriteClient(ManagedChannel channel) {
        super(channel);
        this.stub = IngestorWriteGrpc.newBlockingStub(channel);
    }

    public IngestorWriteClient(IngestorWriteGrpc.IngestorWriteBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public BatchId writeIngestor(String requestId, int queueId, OperationBatch operationBatch) {
        WriteIngestorRequest request = WriteIngestorRequest.newBuilder()
                .setRequestId(requestId)
                .setQueueId(queueId)
                .setOperationBatch(operationBatch.toProto())
                .build();
        WriteIngestorResponse response = this.stub.writeIngestor(request);
        return new BatchId(response.getSnapshotId());
    }
}
