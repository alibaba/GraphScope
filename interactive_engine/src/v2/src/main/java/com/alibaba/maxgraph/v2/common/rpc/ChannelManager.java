package com.alibaba.maxgraph.v2.common.rpc;

import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.NodeDiscovery;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.exception.NodeConnectException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ChannelManager {
    private static final Logger logger = LoggerFactory.getLogger(ChannelManager.class);
    public static final String SCHEME = "node";

    private Configs configs;
    private NameResolver.Factory nameResolverFactory;

    private Set<RoleType> targetRoles = new HashSet<>();
    private Map<RoleType, Map<Integer, ManagedChannel>> roleToChannels;

    private int rpcMaxBytes;

    public ChannelManager(Configs configs, NameResolver.Factory nameResolverFactory) {
        this.configs = configs;
        this.nameResolverFactory = nameResolverFactory;

        this.rpcMaxBytes = CommonConfig.RPC_MAX_BYTES_MB.get(configs) * 1024 * 1024;
    }

    public void start() {
        this.roleToChannels = new HashMap<>();
        for (RoleType role : this.targetRoles) {
            Map<Integer, ManagedChannel> idxToChannel = this.roleToChannels.computeIfAbsent(role, k -> new HashMap<>());
            int count = Integer.valueOf(
                    this.configs.get(String.format(CommonConfig.NODE_COUNT_FORMAT, role.getName()), "0"));
            for (int i = 0; i < count; i++) {
                logger.debug("create channel to role [" + role.getName() + "] #[" + i + "]");
                String uri = SCHEME + "://" + role.getName() + "/" + i;
                ManagedChannel channel = ManagedChannelBuilder.forTarget(uri)
                        .nameResolverFactory(this.nameResolverFactory)
                        .maxInboundMessageSize(this.rpcMaxBytes)
                        .usePlaintext()
                        .build();
                idxToChannel.put(i, channel);
            }
        }
        logger.info("ChannelManager started");
    }

    public void stop() {
        if (roleToChannels != null) {
            for (Map.Entry<RoleType, Map<Integer, ManagedChannel>> roleToEntry : roleToChannels.entrySet()) {
                RoleType role = roleToEntry.getKey();
                Map<Integer, ManagedChannel> idToChannel = roleToEntry.getValue();
                logger.debug("shutdown channels for role [" + role.getName() + "] now");
                for (ManagedChannel channel : idToChannel.values()) {
                    channel.shutdown();
                }
            }
            this.roleToChannels = null;
        }
        logger.info("ChannelManager stopped");
    }

    public void registerRole(RoleType role) {
        this.targetRoles.add(role);
        logger.debug("role [" + role.getName() + "] registered");
    }

    public ManagedChannel getChannel(RoleType role, int idx) {
        Map<Integer, ManagedChannel> idToChannel = this.roleToChannels.get(role);
        if (idToChannel == null) {
            throw new NodeConnectException("invalid role [" + role + "]");
        }
        ManagedChannel channel = idToChannel.get(idx);
        if (channel == null) {
            throw new NodeConnectException("not connected to role [" + role + "] #[" + idx + "]");
        }
        return channel;
    }
}
