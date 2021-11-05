package com.alibaba.graphscope.common.client;

import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;

public abstract class AbstractBroadcastProcessor implements AutoCloseable {
    protected RpcClient rpcClient;

    public abstract void broadcast(PegasusClient.JobRequest request, ResultProcessor resultProcessor);

    public AbstractBroadcastProcessor(RpcChannelFetcher fetcher) {
        this.rpcClient = new RpcClient(fetcher.fetch());
    }
}
