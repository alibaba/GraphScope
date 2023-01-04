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
import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.proto.groot.StoreClearIngestRequest;
import com.alibaba.graphscope.proto.groot.StoreClearIngestResponse;
import com.alibaba.graphscope.proto.groot.StoreIngestGrpc;
import com.alibaba.graphscope.proto.groot.StoreIngestRequest;
import com.alibaba.graphscope.proto.groot.StoreIngestResponse;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.util.Map;

public class StoreIngestClient extends RpcClient {

    private StoreIngestGrpc.StoreIngestStub stub;

    public StoreIngestClient(ManagedChannel channel) {
        super(channel);
        this.stub = StoreIngestGrpc.newStub(channel);
    }

    public void storeIngest(
            String dataPath, Map<String, String> config, CompletionCallback<Void> callback) {
        StoreIngestRequest.Builder builder = StoreIngestRequest.newBuilder();
        builder.setDataPath(dataPath);
        builder.putAllConfig(config);
        this.stub.storeIngest(
                builder.build(),
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

    public void storeClearIngest(CompletionCallback<Void> callback) {
        this.stub.storeClearIngest(
                StoreClearIngestRequest.newBuilder().build(),
                new StreamObserver<StoreClearIngestResponse>() {
                    @Override
                    public void onNext(StoreClearIngestResponse storeClearIngestResponse) {
                        callback.onCompleted(null);
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
