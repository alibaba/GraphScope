package com.alibaba.graphscope.groot.rpc;

import com.alibaba.graphscope.groot.common.RoleType;

import io.grpc.ConnectivityState;
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
        this.channel = manager.getChannel(targetRole, index);
    }

    public RpcChannel(ManagedChannel channel) {
        this.channel = channel;
        manager = null;
        targetRole = null;
        index = -1;
    }

    public ManagedChannel getChannel() {
        ConnectivityState state = channel.getState(false);
        if (state == ConnectivityState.TRANSIENT_FAILURE || state == ConnectivityState.SHUTDOWN) {
            if (manager != null) {
                this.channel = manager.getChannel(targetRole, index);
            }
        }
        return channel;
    }
}
