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
import com.alibaba.graphscope.groot.rpc.RpcChannel;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import com.alibaba.graphscope.proto.groot.*;

import io.grpc.stub.StreamObserver;

import java.util.Map;

public class FrontendStoreClient extends RpcClient {

    public FrontendStoreClient(RpcChannel channel) {
        super(channel);
    }

    private FrontendStoreServiceGrpc.FrontendStoreServiceStub getStub() {
        return FrontendStoreServiceGrpc.newStub(rpcChannel.getChannel());
    }

    public void storeIngest(
            String dataPath, Map<String, String> config, CompletionCallback<Void> callback) {
        IngestDataRequest.Builder builder = IngestDataRequest.newBuilder();
        builder.setDataPath(dataPath);
        builder.putAllConfig(config);
        getStub()
                .storeIngest(
                        builder.build(),
                        new StreamObserver<>() {
                            @Override
                            public void onNext(IngestDataResponse value) {
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

    public void storeClearIngest(String path, CompletionCallback<Void> callback) {
        getStub()
                .storeClearIngest(
                        ClearIngestRequest.newBuilder().setDataPath(path).build(),
                        new StreamObserver<>() {
                            @Override
                            public void onNext(ClearIngestResponse storeClearIngestResponse) {
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

    public void storeCompact(CompletionCallback<Void> callback) {
        getStub()
                .compactDB(
                        CompactDBRequest.newBuilder().build(),
                        new StreamObserver<>() {
                            @Override
                            public void onNext(CompactDBResponse value) {
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

    public void reopenSecondary(CompletionCallback<Void> callback) {
        getStub()
                .reopenSecondary(
                        ReopenSecondaryRequest.newBuilder().build(),
                        new StreamObserver<>() {
                            @Override
                            public void onNext(ReopenSecondaryResponse value) {
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

    public void getStoreState(CompletionCallback<GetStoreStateResponse> callback) {
        getStub()
                .getState(
                        GetStoreStateRequest.newBuilder().build(),
                        new StreamObserver<>() {
                            @Override
                            public void onNext(GetStoreStateResponse value) {
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

    public void replayRecordsV2(
            ReplayRecordsRequestV2 request, CompletionCallback<ReplayRecordsResponseV2> callback) {
        getStub()
                .replayRecordsV2(
                        request,
                        new StreamObserver<ReplayRecordsResponseV2>() {
                            @Override
                            public void onNext(ReplayRecordsResponseV2 value) {
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
