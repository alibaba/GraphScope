/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.comm.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.ImmediateEventExecutor;

import org.apache.giraph.comm.WorkerInfo;
import org.apache.giraph.comm.netty.handler.NettyServerHandler;
import org.apache.giraph.comm.requests.NettyMessageDecoder;
import org.apache.giraph.comm.requests.NettyMessageEncoder;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.AggregatorManager;
import org.apache.giraph.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/** This server uses Netty and will implement all Giraph communication */
public class NettyServer {

    private static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    /** Boss eventloop group */
    private final EventLoopGroup bossGroup;
    /** Worker eventloop group */
    private final EventLoopGroup workerGroup;

    private int maxPoolSize;
    private ImmutableClassesGiraphConfiguration conf;
    private AtomicInteger msgCounter;
    /** Accepted channels */
    private final ChannelGroup accepted = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);

    public NettyServer(
            ImmutableClassesGiraphConfiguration conf,
            AggregatorManager aggregatorManager,
            WorkerInfo workerInfo,
            final Thread.UncaughtExceptionHandler exceptionHandler) {
        msgCounter = new AtomicInteger(0);
        this.conf = conf;

        //        maxPoolSize = GiraphConstants.NETTY_SERVER_THREADS.get(conf);
        maxPoolSize = 4;

        bossGroup =
                new NioEventLoopGroup(
                        4,
                        ThreadUtils.createThreadFactory("netty-server-boss-%d", exceptionHandler));

        workerGroup =
                new NioEventLoopGroup(
                        maxPoolSize,
                        ThreadUtils.createThreadFactory(
                                "netty-server-worker-%d", exceptionHandler));
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.childOption(ChannelOption.SO_KEEPALIVE, true);
            b.childOption(ChannelOption.TCP_NODELAY, true);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel ch) {
                                    ChannelPipeline p = ch.pipeline();
                                    p.addLast(
                                            new ChannelInboundHandlerAdapter() {
                                                @Override
                                                public void channelActive(ChannelHandlerContext ctx)
                                                        throws Exception {
                                                    accepted.add(ctx.channel());
                                                    ctx.fireChannelActive();
                                                }
                                            });
                                    p.addLast(new NettyMessageEncoder());
                                    p.addLast(new NettyMessageDecoder());
                                    p.addLast(
                                            new NettyServerHandler(aggregatorManager, msgCounter));
                                }
                            });

            // Bind and start to accept incoming connections.
            //            channel = ;
            accepted.add(b.bind(workerInfo.getInitPort()).sync().channel());

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
        }
    }

    public AtomicInteger getMsgNo() {
        return msgCounter;
    }

    public void close() {
        try {
            accepted.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("closing server");
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        logger.info("close thread groups");
        // try {
        //    channelFuture.channel().closeFuture().sync();
        // } catch (InterruptedException e) {
        //    e.printStackTrace();
        // }

        logger.info("Successfully close server");
    }
}
