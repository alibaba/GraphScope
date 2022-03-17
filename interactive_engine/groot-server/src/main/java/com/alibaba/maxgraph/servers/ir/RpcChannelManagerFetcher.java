package com.alibaba.maxgraph.servers.ir;

import com.alibaba.graphscope.common.client.RpcChannelFetcher;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.pegasus.RpcChannel;

import java.util.ArrayList;
import java.util.List;

public class RpcChannelManagerFetcher implements RpcChannelFetcher {
    private ChannelManager manager;
    private int pegasusServerNum;
    private RoleType targetRole;

    public RpcChannelManagerFetcher(
            ChannelManager manager, int pegasusServerNum, RoleType targetRole) {
        this.manager = manager;
        this.pegasusServerNum = pegasusServerNum;
        this.targetRole = targetRole;
        this.manager.registerRole(this.targetRole);
    }

    @Override
    public List<RpcChannel> fetch() {
        List<RpcChannel> channels = new ArrayList<>();
        for (int i = 0; i < pegasusServerNum; ++i) {
            channels.add(new RpcChannel(manager.getChannel(this.targetRole, i)));
        }
        return channels;
    }

    @Override
    public boolean isDynamic() {
        return true;
    }
}
