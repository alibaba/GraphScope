package com.alibaba.graphscope.gaia.broadcast.channel;

import com.alibaba.pegasus.RpcChannel;

import java.util.List;

public interface RpcChannelFetcher {
    List<RpcChannel> fetch();
}
