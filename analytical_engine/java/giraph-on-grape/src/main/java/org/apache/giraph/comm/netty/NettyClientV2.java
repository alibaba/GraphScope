/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.comm.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.comm.netty.handler.NettyClientHandlerV2;
import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.comm.requests.NettyMessageDecoder;
import org.apache.giraph.comm.requests.NettyMessageEncoder;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The implementation class which do responsible for all netty-related stuff.
 */
public class NettyClientV2 {

    /**
     * 30 seconds to connect by default
     */
    public static final int MAX_CONNECTION_MILLISECONDS_DEFAULT = 30 * 1000;

    public static final int SIZE = 256;
    private static Logger logger = LoggerFactory.getLogger(NettyClientV2.class);
    private WorkerInfo workerInfo;
    private EventLoopGroup workGroup;
    //    private ChannelFuture channelFuture;
    private Channel channel;
    private ImmutableClassesGiraphConfiguration conf;
    private AggregatorManager aggregatorManager;
    private NettyClientHandlerV2 handler;
    private Map<String, Writable> result;

    public NettyClientV2(
            ImmutableClassesGiraphConfiguration conf,
            AggregatorManager aggregatorManager,
            WorkerInfo workerInfo,
            final Thread.UncaughtExceptionHandler exceptionHandler) {
        result = new HashMap<>();
        this.workerInfo = workerInfo;
        this.conf = conf;
        this.aggregatorManager = aggregatorManager;
        workGroup =
                new NioEventLoopGroup(
                        1,
                        ThreadUtils.createThreadFactory(
                                "netty-client-worker-%d", exceptionHandler));

        Bootstrap b = new Bootstrap();
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.group(workGroup)
                .channel(NioSocketChannel.class)
                .handler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                ChannelPipeline p = ch.pipeline();
                                p.addLast(new NettyMessageEncoder());
                                p.addLast(new NettyMessageDecoder());
                                p.addLast(
                                        new NettyClientHandlerV2(
                                                aggregatorManager, workerInfo.getWorkerId()));
                            }
                        });
        // Make the connection attempt.
        ChannelFuture future;

        int failureTime = 0;
        while (failureTime < 10) {
            try {
                future = b.connect(workerInfo.getHost(), workerInfo.getInitPort()).sync();
            } catch (Exception e) {
                e.printStackTrace();
                failureTime += 1;
                logger.info("failed for " + failureTime + " times");
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException ee) {
                    ee.printStackTrace();
                }
                continue;
            }
            if (!future.isSuccess() || !future.channel().isOpen()) {
                logger.info("failed for " + failureTime + " times");
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                failureTime += 1;
            } else {
                logger.info("success for " + failureTime + " times");
                channel = future.channel();
                break;
            }
        }
        if (failureTime >= 10) {
            logger.error("connection failed");
            return;
        }

        handler = (NettyClientHandlerV2) channel.pipeline().last();
    }

    public boolean isConnected() {
        return Objects.nonNull(channel);
    }

    public Future<NettyMessage> sendMessage(NettyMessage request) {
        return handler.sendMessage(request);
    }

    public NettyMessage getResponse() {
        return handler.getResponse();
    }

    public void close() {
        try {
            if (!Objects.isNull(channel)) {
                channel.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        workGroup.shutdownGracefully();
        logger.info("shut down client");
    }
}
