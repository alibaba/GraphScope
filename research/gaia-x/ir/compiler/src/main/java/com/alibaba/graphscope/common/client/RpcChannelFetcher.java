package com.alibaba.graphscope.common.client;

import com.alibaba.pegasus.RpcChannel;

import java.util.List;

public interface RpcChannelFetcher {
    List<RpcChannel> fetch();
}
