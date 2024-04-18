package com.alibaba.graphscope.groot.rpc;

import com.alibaba.graphscope.groot.common.RoleType;

import io.grpc.ManagedChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcChannel {
    public static final Logger logger = LoggerFactory.getLogger(RpcChannel.class);
    private final ChannelManager manager;
    private final RoleType targetRole;
    private final int index;

    private ManagedChannel channel;

    public RpcChannel(ChannelManager manager, RoleType targetRole, int index) {
        this.manager = manager;
        this.targetRole = targetRole;
        this.index = index;
        this.channel = null;
    }

    public RpcChannel(ManagedChannel channel) {
        this.channel = channel;
        manager = null;
        targetRole = null;
        index = -1;
    }

    public ManagedChannel getChannel() {
        if (manager != null) {
            return manager.getChannel(targetRole, index);
        } else {
            return channel;
        }
    }
}
