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

import com.alibaba.maxgraph.proto.v2.StoreIngestGrpc;
import com.alibaba.maxgraph.proto.v2.StoreIngestRequest;
import com.alibaba.maxgraph.proto.v2.StoreIngestResponse;
import com.alibaba.maxgraph.groot.common.CompletionCallback;
import com.alibaba.maxgraph.groot.common.rpc.RpcClient;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class StoreIngestClient extends RpcClient {

    private StoreIngestGrpc.StoreIngestStub stub;

    public StoreIngestClient(ManagedChannel channel) {
        super(channel);
        this.stub = StoreIngestGrpc.newStub(channel);
    }

    public void storeIngest(String dataPath, CompletionCallback<Void> callback) {
        StoreIngestRequest req = StoreIngestRequest.newBuilder().setDataPath(dataPath).build();
        this.stub.storeIngest(req, new StreamObserver<StoreIngestResponse>() {
            @Override
            public void onNext(StoreIngestResponse value) {
                callback.onCompleted(null);
            }

            @Override
            public void onError(Throwable t) {
                callback.onError(t);
            }

            @Override
            public void onCompleted() {

            }
        });
    }
}
