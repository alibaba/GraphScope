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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.common.schema.wrapper.GraphDef;
import com.alibaba.graphscope.groot.rpc.RpcChannel;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.proto.groot.*;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

public class StoreSchemaClient extends RpcClient {
    public StoreSchemaClient(RpcChannel channel) {
        super(channel);
    }

    public StoreSchemaClient(StoreSchemaGrpc.StoreSchemaBlockingStub stub) {
        super((ManagedChannel) stub.getChannel());
    }

    private StoreSchemaGrpc.StoreSchemaBlockingStub getStub() {
        return StoreSchemaGrpc.newBlockingStub(rpcChannel.getChannel());
    }

    private StoreSchemaGrpc.StoreSchemaStub getAsyncStub() {
        return StoreSchemaGrpc.newStub(rpcChannel.getChannel());
    }

    public GraphDef fetchSchema() {
        StoreSchemaGrpc.StoreSchemaBlockingStub stub = getStub();
        FetchSchemaResponse response = stub.fetchSchema(FetchSchemaRequest.newBuilder().build());
        return GraphDef.parseProto(response.getGraphDef());
    }

    public void fetchStatistics(CompletionCallback<FetchStatisticsResponse> callback) {
        long snapshotId = Long.MAX_VALUE - 1;
        FetchStatisticsRequest request =
                FetchStatisticsRequest.newBuilder().setSnapshotId(snapshotId).build();
        getAsyncStub()
                .fetchStatistics(
                        request,
                        new StreamObserver<>() {
                            @Override
                            public void onNext(FetchStatisticsResponse value) {
                                callback.onCompleted(value);
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
