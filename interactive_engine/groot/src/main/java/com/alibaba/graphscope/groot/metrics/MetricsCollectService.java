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
package com.alibaba.graphscope.groot.metrics;

import com.alibaba.maxgraph.proto.groot.CollectMetricsRequest;
import com.alibaba.maxgraph.proto.groot.CollectMetricsResponse;
import com.alibaba.maxgraph.proto.groot.MetricsCollectGrpc;
import io.grpc.stub.StreamObserver;

import java.util.Map;

public class MetricsCollectService extends MetricsCollectGrpc.MetricsCollectImplBase {

    private MetricsCollector metricsCollector;

    public MetricsCollectService(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    @Override
    public void collectMetrics(CollectMetricsRequest request, StreamObserver<CollectMetricsResponse> responseObserver) {
        Map<String, String> metrics = metricsCollector.collectMetrics();
        responseObserver.onNext(CollectMetricsResponse.newBuilder().putAllMetrics(metrics).build());
        responseObserver.onCompleted();
    }
}
