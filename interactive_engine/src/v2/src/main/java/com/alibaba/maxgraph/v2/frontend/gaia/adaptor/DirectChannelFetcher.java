package com.alibaba.maxgraph.v2.frontend.gaia.adaptor;

import com.alibaba.graphscope.gaia.broadcast.channel.RpcChannelFetcher;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.pegasus.RpcChannel;

import java.util.ArrayList;
import java.util.List;

public class DirectChannelFetcher implements RpcChannelFetcher {
    private ChannelManager manager;
    private int pegasusServerNum;
    private RoleType targetRole;

    public DirectChannelFetcher(ChannelManager manager, int pegasusServerNum, RoleType targetRole) {
        this.manager = manager;
        this.pegasusServerNum = pegasusServerNum;
        this.targetRole = targetRole;
        manager.registerRole(this.targetRole);
    }

    @Override
    public List<RpcChannel> fetch() {
        List<RpcChannel> channels = new ArrayList<>();
        for (int i = 0; i < pegasusServerNum; ++i) {
            channels.add(new RpcChannel(manager.getChannel(this.targetRole, i)));
        }
        return channels;
    }
}
