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

import com.alibaba.maxgraph.proto.groot.StoreIngestGrpc;
import com.alibaba.maxgraph.proto.groot.StoreIngestRequest;
import com.alibaba.maxgraph.proto.groot.StoreIngestResponse;
import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.rpc.RpcClient;
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
        this.stub.storeIngest(
                req,
                new StreamObserver<StoreIngestResponse>() {
                    @Override
                    public void onNext(StoreIngestResponse value) {
                        callback.onCompleted(null);
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
