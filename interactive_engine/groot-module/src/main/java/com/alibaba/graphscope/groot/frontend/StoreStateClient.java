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
import com.alibaba.graphscope.proto.groot.*;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.util.Map;

public class StoreStateClient extends RpcClient {

    private StateServiceGrpc.StateServiceStub stub;

    public StoreStateClient(ManagedChannel channel) {
        super(channel);
        this.stub = StateServiceGrpc.newStub(channel);
    }

    public void getStoreState(CompletionCallback<Void> callback) {
        GetStoreStateRequest.Builder builder = GetStoreStateRequest.newBuilder();

        this.stub.getState(
                builder.build(),
                new StreamObserver<GetStoreStateResponse>() {
                    @Override
                    public void onNext(GetStoreStateResponse value) {
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



