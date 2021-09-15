package com.alibaba.graphscope.gaia.broadcast.channel;

import com.alibaba.pegasus.RpcChannel;

import java.util.Collections;
import java.util.List;

public abstract class AsyncRpcChannelFetcher implements RpcChannelFetcher {
    @Override
    public List<RpcChannel> fetch() {
        return Collections.EMPTY_LIST;
    }

    abstract public List<RpcChannel> refresh();
}
