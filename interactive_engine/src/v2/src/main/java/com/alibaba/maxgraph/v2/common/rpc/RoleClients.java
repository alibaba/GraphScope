/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.v2.common.rpc;

import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * This class maintains connections with nodes of a role, provides RpcClient for client use
 *
 * @param <T>
 */
public class RoleClients<T extends RpcClient> {

    private ChannelManager channelManager;
    private RoleType targetRole;

    private Map<Integer, T> clients;
    private Function<ManagedChannel, T> clientBuilder;

    public RoleClients(ChannelManager channelManager, RoleType targetRole, Function<ManagedChannel, T> clientBuilder) {
        this.channelManager = channelManager;
        this.targetRole = targetRole;
        this.clientBuilder = clientBuilder;
        this.clients = new ConcurrentHashMap<>();
        channelManager.registerRole(targetRole);
    }

    public T getClient(int clientId) {
        T client = this.clients.get(clientId);
        if (client == null) {
            synchronized (this) {
                client = this.clients.get(clientId);
                if (client == null) {
                    ManagedChannel channel = this.channelManager.getChannel(targetRole, clientId);
                    client = clientBuilder.apply(channel);
                    this.clients.put(clientId, client);
                }
            }
        }
        return client;
    }

}
