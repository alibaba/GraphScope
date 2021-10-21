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

import com.alibaba.maxgraph.common.config.*;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.maxgraph.compiler.api.exception.NodeConnectException;
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
            Map<Integer, ManagedChannel> idxToChannel =
                    this.roleToChannels.computeIfAbsent(role, k -> new HashMap<>());
            int count =
                    Integer.valueOf(
                            this.configs.get(
                                    String.format(CommonConfig.NODE_COUNT_FORMAT, role.getName()),
                                    "0"));
            if (CommonConfig.DISCOVERY_MODE.get(configs).equalsIgnoreCase("file")) {
                String hostTemplate;
                int port;
                switch (role) {
                    case FRONTEND:
                        hostTemplate = DiscoveryConfig.DNS_NAME_PREFIX_FRONTEND.get(configs);
                        port = CommonConfig.RPC_PORT.get(configs);
                        break;
                    case INGESTOR:
                        hostTemplate = DiscoveryConfig.DNS_NAME_PREFIX_INGESTOR.get(configs);
                        port = CommonConfig.RPC_PORT.get(configs);
                        break;
                    case COORDINATOR:
                        hostTemplate = DiscoveryConfig.DNS_NAME_PREFIX_COORDINATOR.get(configs);
                        port = CommonConfig.RPC_PORT.get(configs);
                        break;
                    case STORE:
                        hostTemplate = DiscoveryConfig.DNS_NAME_PREFIX_STORE.get(configs);
                        port = CommonConfig.RPC_PORT.get(configs);
                        break;
                    case EXECUTOR_GRAPH:
                    case EXECUTOR_MANAGE:
                        hostTemplate = DiscoveryConfig.DNS_NAME_PREFIX_STORE.get(configs);
                        port = StoreConfig.EXECUTOR_GRAPH_PORT.get(configs);
                        break;
                    case EXECUTOR_QUERY:
                        hostTemplate = DiscoveryConfig.DNS_NAME_PREFIX_STORE.get(configs);
                        port = StoreConfig.EXECUTOR_QUERY_PORT.get(configs);
                        break;
                    case EXECUTOR_ENGINE:
                        hostTemplate = DiscoveryConfig.DNS_NAME_PREFIX_STORE.get(configs);
                        port = StoreConfig.EXECUTOR_ENGINE_PORT.get(configs);
                        break;
                    case GAIA_ENGINE:
                        hostTemplate = DiscoveryConfig.DNS_NAME_PREFIX_STORE.get(configs);
                        port = GaiaConfig.GAIA_ENGINE_PORT.get(configs);
                        break;
                    case GAIA_RPC:
                        hostTemplate = DiscoveryConfig.DNS_NAME_PREFIX_STORE.get(configs);
                        port = GaiaConfig.GAIA_RPC_PORT.get(configs);
                        break;
                    default:
                        throw new IllegalArgumentException("invalid role [" + role + "]");
                }
                for (int i = 0; i < count; i++) {
                    String host = hostTemplate.replace("{}", String.valueOf(i));
                    logger.info(
                            "create channel to role ["
                                    + role.getName()
                                    + "] #["
                                    + i
                                    + "]. host ["
                                    + host
                                    + "], port ["
                                    + port
                                    + "]");
                    ManagedChannel channel =
                            ManagedChannelBuilder.forAddress(host, port)
                                    .maxInboundMessageSize(this.rpcMaxBytes)
                                    .usePlaintext()
                                    .build();
                    idxToChannel.put(i, channel);
                }
            } else {
                for (int i = 0; i < count; i++) {
                    logger.debug("create channel to role [" + role.getName() + "] #[" + i + "]");
                    String uri = SCHEME + "://" + role.getName() + "/" + i;
                    ManagedChannel channel =
                            ManagedChannelBuilder.forTarget(uri)
                                    .nameResolverFactory(this.nameResolverFactory)
                                    .maxInboundMessageSize(this.rpcMaxBytes)
                                    .usePlaintext()
                                    .build();
                    idxToChannel.put(i, channel);
                }
            }
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
