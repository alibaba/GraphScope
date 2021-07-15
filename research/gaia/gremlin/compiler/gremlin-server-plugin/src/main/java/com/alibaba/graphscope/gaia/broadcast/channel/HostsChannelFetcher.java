package com.alibaba.graphscope.gaia.broadcast.channel;

import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.pegasus.RpcChannel;

import java.util.ArrayList;
import java.util.List;

public class HostsChannelFetcher implements RpcChannelFetcher {
    private GaiaConfig config;

    public HostsChannelFetcher(GaiaConfig config) {
        this.config = config;
    }

    @Override
    public List<RpcChannel> fetch() {
        List<String> hostAddresses = config.getPegasusPhysicalHosts();
        List<RpcChannel> rpcChannels = new ArrayList<>();
        hostAddresses.forEach(k -> {
            String[] host = k.split(":");
            rpcChannels.add(new RpcChannel(host[0], Integer.valueOf(host[1])));
        });
        return rpcChannels;
    }
}
