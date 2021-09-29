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
package com.alibaba.maxgraph.groot.store;

import com.alibaba.maxgraph.proto.v2.StoreDataBatchPb;
import com.alibaba.maxgraph.proto.v2.StoreWriteGrpc;
import com.alibaba.maxgraph.proto.v2.WriteStoreRequest;
import com.alibaba.maxgraph.proto.v2.WriteStoreResponse;
import com.alibaba.maxgraph.groot.common.StoreDataBatch;
import io.grpc.stub.StreamObserver;

public class StoreWriteService extends StoreWriteGrpc.StoreWriteImplBase {

    private WriterAgent writerAgent;

    public StoreWriteService(WriterAgent writerAgent) {
        this.writerAgent = writerAgent;
    }

    @Override
    public void writeStore(WriteStoreRequest request, StreamObserver<WriteStoreResponse> responseObserver) {
        StoreDataBatchPb batchProto = request.getBatch();
        try {
            boolean success = writerAgent.writeStore(StoreDataBatch.parseProto(batchProto));
            WriteStoreResponse response = WriteStoreResponse.newBuilder()
                    .setSuccess(success)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
