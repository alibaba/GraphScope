package com.alibaba.maxgraph.v2.common.rpc;

import io.grpc.ManagedChannel;

import java.util.concurrent.TimeUnit;

public abstract class RpcClient implements AutoCloseable {

    protected ManagedChannel channel;

    public RpcClient(ManagedChannel channel) {
        this.channel = channel;
    }

    public void close() {
        this.channel.shutdown();
        try {
            this.channel.awaitTermination(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // Ignore
        }
    }

}
