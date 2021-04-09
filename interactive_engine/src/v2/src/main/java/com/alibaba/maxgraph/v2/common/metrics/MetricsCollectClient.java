package com.alibaba.maxgraph.v2.common.metrics;

import com.alibaba.maxgraph.proto.v2.CollectMetricsRequest;
import com.alibaba.maxgraph.proto.v2.CollectMetricsResponse;
import com.alibaba.maxgraph.proto.v2.MetricsCollectGrpc;
import com.alibaba.maxgraph.v2.common.CompletionCallback;
import com.alibaba.maxgraph.v2.common.rpc.RpcClient;
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
        this.stub.collectMetrics(CollectMetricsRequest.newBuilder().build(),
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
            public void onCompleted() {

            }
        });
    }
}
