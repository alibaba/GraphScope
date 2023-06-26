/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.client.channel;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.pegasus.RpcChannel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * rpc implementation of {@link ChannelFetcher}, init rpc from local config
 */
public class HostsRpcChannelFetcher implements ChannelFetcher<RpcChannel> {
    private Configs config;

    public HostsRpcChannelFetcher(Configs config) {
        this.config = config;
    }

    @Override
    public List<RpcChannel> fetch() {
        String hosts = PegasusConfig.PEGASUS_HOSTS.get(config);
        String[] hostsArr = hosts.split(",");
        List<String> hostAddresses = Arrays.asList(hostsArr);
        List<RpcChannel> rpcChannels = new ArrayList<>();
        hostAddresses.forEach(
                k -> {
                    String[] host = k.split(":");
                    rpcChannels.add(new RpcChannel(host[0], Integer.valueOf(host[1])));
                });
        return rpcChannels;
    }

    @Override
    public Type getType() {
        return Type.RPC;
    }
}
