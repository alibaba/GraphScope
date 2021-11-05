package com.alibaba.graphscope.common.client;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.pegasus.RpcChannel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HostsChannelFetcher implements RpcChannelFetcher {
    private Configs config;

    public HostsChannelFetcher(Configs config) {
        this.config = config;
    }

    @Override
    public List<RpcChannel> fetch() {
        String hosts = PegasusConfig.PEGASUS_HOSTS.get(config);
        String[] hostsArr = hosts.split(",");
        List<String> hostAddresses = Arrays.asList(hostsArr);
        List<RpcChannel> rpcChannels = new ArrayList<>();
        hostAddresses.forEach(k -> {
            String[] host = k.split(":");
            rpcChannels.add(new RpcChannel(host[0], Integer.valueOf(host[1])));
        });
        return rpcChannels;
    }
}
