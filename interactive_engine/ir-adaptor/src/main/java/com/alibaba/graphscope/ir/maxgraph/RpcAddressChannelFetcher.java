package com.alibaba.graphscope.ir.maxgraph;

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.maxgraph.common.rpc.RpcAddressFetcher;
import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.pegasus.RpcChannel;

import java.util.List;
import java.util.stream.Collectors;

public class RpcAddressChannelFetcher implements RpcChannelFetcher {
    private RpcAddressFetcher addressFetcher;

    public RpcAddressChannelFetcher(RpcAddressFetcher addressFetcher) {
        this.addressFetcher = addressFetcher;
    }

    @Override
    public List<RpcChannel> fetch() {
        List<Endpoint> endpoints = addressFetcher.getServiceAddress();
        return endpoints.stream().map(k -> new RpcChannel(k.getIp(), k.getRuntimeCtrlAndAsyncPort())).collect(Collectors.toList());
    }

    @Override
    public boolean isDynamic() {
        return true;
    }
}
