/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.rpc;

import com.alibaba.graphscope.groot.Utils;
import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.*;
import com.alibaba.graphscope.groot.common.exception.NetworkFailureException;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ChannelManager {
    private static final Logger logger = LoggerFactory.getLogger(ChannelManager.class);
    public static final String SCHEME = "node";

    private final Configs configs;
    private final NameResolver.Factory nameResolverFactory;

    private final Set<RoleType> targetRoles = new HashSet<>();
    private Map<RoleType, Map<Integer, ManagedChannel>> roleToChannels;

    private final int rpcMaxBytes;

    private final GrpcTelemetry grpcTelemetry;

    public ChannelManager(Configs configs, NameResolver.Factory nameResolverFactory) {
        this.configs = configs;
        this.nameResolverFactory = nameResolverFactory;
        this.rpcMaxBytes = CommonConfig.RPC_MAX_BYTES_MB.get(configs) * 1024 * 1024;
        OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
        this.grpcTelemetry = GrpcTelemetry.create(openTelemetry);
    }

    public void start() {
        this.roleToChannels = new HashMap<>();
        for (RoleType role : this.targetRoles) {
            Map<Integer, ManagedChannel> idxToChannel =
                    roleToChannels.computeIfAbsent(role, k -> new HashMap<>());
            String nodeCount =
                    configs.get(String.format(CommonConfig.NODE_COUNT_FORMAT, role.getName()));
            int count = Integer.parseInt(nodeCount);
            String discoveryMode = CommonConfig.DISCOVERY_MODE.get(configs).toLowerCase();
            if (discoveryMode.equals("zookeeper")) {
                for (int i = 0; i < count; i++) {
                    logger.info("Create channel to role {} #{}", role.getName(), i);
                    String uri = SCHEME + "://" + role.getName() + "/" + i;
                    ManagedChannel channel =
                            ManagedChannelBuilder.forTarget(uri)
                                    .nameResolverFactory(this.nameResolverFactory)
                                    .maxInboundMessageSize(this.rpcMaxBytes)
                                    .usePlaintext()
                                    .intercept(grpcTelemetry.newClientInterceptor())
                                    .build();
                    idxToChannel.put(i, channel);
                }
            }
            // channel in file discovery mode will be lazy created
        }
        logger.info("ChannelManager started");
    }

    public void stop() {
        if (roleToChannels != null) {
            for (Map.Entry<RoleType, Map<Integer, ManagedChannel>> roleToEntry :
                    roleToChannels.entrySet()) {
                RoleType role = roleToEntry.getKey();
                Map<Integer, ManagedChannel> idToChannel = roleToEntry.getValue();
                logger.debug("shutdown channels for role [" + role.getName() + "] now");
                for (ManagedChannel channel : idToChannel.values()) {
                    channel.shutdown();
                }
            }
            this.roleToChannels = null;
        }
        logger.debug("ChannelManager stopped");
    }

    public void registerRole(RoleType role) {
        this.targetRoles.add(role);
        logger.debug("role [" + role.getName() + "] registered");
    }

    private ManagedChannel createChannel(RoleType role, int idx) {
        String host = Utils.getHostTemplate(configs, role).replace("{}", String.valueOf(idx));
        int port = Utils.getPort(configs, role, idx);
        logger.info("Create channel to {}#{}, {}:{}", role.getName(), idx, host, port);
        ManagedChannel channel =
                ManagedChannelBuilder.forAddress(host, port)
                        .maxInboundMessageSize(this.rpcMaxBytes)
                        .usePlaintext()
                        .intercept(grpcTelemetry.newClientInterceptor())
                        .build();
        return channel;
    }

    public ManagedChannel getChannel(RoleType role, int idx) {
        Map<Integer, ManagedChannel> idToChannel = this.roleToChannels.get(role);
        if (idToChannel == null) {
            throw new NetworkFailureException("invalid role [" + role + "]");
        }
        // to avoid thread competition
        if (idToChannel.get(idx) == null) {
            synchronized (this) {
                if (idToChannel.get(idx) == null) {
                    ManagedChannel channel = createChannel(role, idx);
                    idToChannel.put(idx, channel);
                }
            }
        }
        return idToChannel.get(idx);
    }
}
