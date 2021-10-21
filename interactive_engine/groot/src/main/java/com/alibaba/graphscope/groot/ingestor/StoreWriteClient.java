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
package com.alibaba.graphscope.groot.ingestor;

import com.alibaba.maxgraph.proto.groot.StoreDataBatchPb;
import com.alibaba.maxgraph.proto.groot.StoreWriteGrpc;
import com.alibaba.maxgraph.proto.groot.WriteStoreRequest;
import com.alibaba.maxgraph.proto.groot.WriteStoreResponse;
import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

/** ingestor -> store */
public class StoreWriteClient extends RpcClient {

    private StoreWriteGrpc.StoreWriteStub stub;

    public StoreWriteClient(ManagedChannel channel) {
        super(channel);
        this.stub = StoreWriteGrpc.newStub(channel);
    }

    public StoreWriteClient(StoreWriteGrpc.StoreWriteStub stub) {
        super((ManagedChannel) stub.getChannel());
        this.stub = stub;
    }

    public void writeStore(StoreDataBatch storeDataBatch, CompletionCallback<Integer> callback) {
        StoreDataBatchPb batchPb = storeDataBatch.toProto();
        WriteStoreRequest req = WriteStoreRequest.newBuilder().setBatch(batchPb).build();
        stub.writeStore(
                req,
                new StreamObserver<WriteStoreResponse>() {
                    @Override
                    public void onNext(WriteStoreResponse writeStoreResponse) {
                        boolean success = writeStoreResponse.getSuccess();
                        if (success) {
                            callback.onCompleted(batchPb.getSerializedSize());
                        } else {
                            onError(new RuntimeException("store buffer is full"));
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        callback.onError(throwable);
                    }

                    @Override
                    public void onCompleted() {}
                });
    }
}
