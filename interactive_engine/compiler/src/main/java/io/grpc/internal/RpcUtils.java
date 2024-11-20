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

package io.grpc.internal;

import com.alibaba.graphscope.gremlin.Utils;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyUtils;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

public class RpcUtils {
    public static @Nullable ManagedChannel getDelegateChannel(Channel channel) {
        if (channel instanceof ForwardingManagedChannel) {
            ManagedChannel delegate =
                    Utils.getFieldValue(ForwardingManagedChannel.class, channel, "delegate");
            return getDelegateChannel(delegate);
        }
        return (channel instanceof ManagedChannel) ? (ManagedChannel) channel : null;
    }

    public static io.netty.channel.Channel getNettyChannel(ManagedChannel grpcChannel) {
        if (grpcChannel instanceof ManagedChannelImpl) {
            ManagedChannelImpl channelImpl = (ManagedChannelImpl) grpcChannel;
            Set<InternalSubchannel> subChannels =
                    Utils.getFieldValue(ManagedChannelImpl.class, channelImpl, "subchannels");
            if (subChannels != null && !subChannels.isEmpty()) {
                ClientTransport transport = subChannels.iterator().next().getTransport();
                while (transport instanceof ForwardingConnectionClientTransport) {
                    transport = ((ForwardingConnectionClientTransport) transport).delegate();
                }
                return NettyUtils.getNettyChannel(transport);
            }
        }
        return null;
    }
}
