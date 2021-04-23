package com.alibaba.maxgraph.v2.common.metrics;

import com.alibaba.maxgraph.proto.v2.CollectMetricsRequest;
import com.alibaba.maxgraph.proto.v2.CollectMetricsResponse;
import com.alibaba.maxgraph.proto.v2.MetricsCollectGrpc;
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
