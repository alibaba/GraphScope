/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.client.metric;

import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.metric.Metric;
import com.alibaba.pegasus.RpcChannel;
import com.google.common.collect.Maps;

import io.grpc.ManagedChannel;
import io.grpc.internal.RpcUtils;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.SingleThreadEventExecutor;

import java.util.List;
import java.util.Map;

public class RpcExecutorMetric implements Metric<Map> {
    private final ChannelFetcher<RpcChannel> channelFetcher;

    public RpcExecutorMetric(ChannelFetcher<RpcChannel> channelFetcher) {
        this.channelFetcher = channelFetcher;
    }

    @Override
    public Key getKey() {
        return KeyFactory.RPC_CHANNELS_EXECUTOR_QUEUE;
    }

    @Override
    public Map getValue() {
        List<RpcChannel> channels = channelFetcher.fetch();
        Map<String, Integer> values = Maps.newHashMap();
        channels.forEach(
                k -> {
                    ManagedChannel channel = RpcUtils.getDelegateChannel(k.getChannel());
                    int queueSize = ValueFactory.INVALID_INT;
                    Channel nettyChannel = RpcUtils.getNettyChannel(channel);
                    if (nettyChannel != null) {
                        EventLoop eventLoop = nettyChannel.eventLoop();
                        if (eventLoop instanceof SingleThreadEventExecutor) {
                            queueSize = ((SingleThreadEventExecutor) eventLoop).pendingTasks();
                        }
                    }
                    values.put(k.getChannel().authority(), queueSize);
                });
        return values;
    }
}
