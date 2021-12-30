/*
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
package com.alibaba.maxgraph.servers.gaia;

import com.alibaba.graphscope.gaia.broadcast.channel.AsyncRpcChannelFetcher;
import com.alibaba.graphscope.groot.rpc.ChannelManager;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.pegasus.RpcChannel;

import java.util.ArrayList;
import java.util.List;

public class ChannelManagerFetcher extends AsyncRpcChannelFetcher {
    private ChannelManager manager;
    private int pegasusServerNum;
    private RoleType targetRole;

    public ChannelManagerFetcher(
            ChannelManager manager, int pegasusServerNum, RoleType targetRole) {
        this.manager = manager;
        this.pegasusServerNum = pegasusServerNum;
        this.targetRole = targetRole;
        manager.registerRole(this.targetRole);
    }

    @Override
    public List<RpcChannel> refresh() {
        List<RpcChannel> channels = new ArrayList<>();
        for (int i = 0; i < pegasusServerNum; ++i) {
            channels.add(new RpcChannel(manager.getChannel(this.targetRole, i)));
        }
        return channels;
    }
}
