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
package com.alibaba.graphscope.groot.metrics;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.maxgraph.proto.groot.CollectMetricsRequest;
import com.alibaba.maxgraph.proto.groot.CollectMetricsResponse;
import com.alibaba.maxgraph.proto.groot.MetricsCollectGrpc;
import com.alibaba.graphscope.groot.rpc.RpcClient;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.util.Map;

public class MetricsCollectClient extends RpcClient {

    private MetricsCollectGrpc.MetricsCollectStub stub;

    public MetricsCollectClient(ManagedChannel channel) {
        super(channel);
        this.stub = MetricsCollectGrpc.newStub(channel);
    }

    public void collectMetrics(CompletionCallback<Map<String, String>> callback) {
        this.stub.collectMetrics(
                CollectMetricsRequest.newBuilder().build(),
                new StreamObserver<CollectMetricsResponse>() {
                    @Override
                    public void onNext(CollectMetricsResponse value) {
                        Map<String, String> metricsMap = value.getMetricsMap();
                        callback.onCompleted(metricsMap);
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
