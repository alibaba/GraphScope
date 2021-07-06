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
package com.alibaba.maxgraph.gaia;

import com.alibaba.graphscope.gaia.broadcast.channel.RpcChannelFetcher;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.rpc.ChannelManager;
import com.alibaba.pegasus.RpcChannel;

import java.util.ArrayList;
import java.util.List;

public class DirectChannelFetcher implements RpcChannelFetcher{
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
