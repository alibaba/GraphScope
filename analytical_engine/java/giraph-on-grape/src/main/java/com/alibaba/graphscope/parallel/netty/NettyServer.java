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
package com.alibaba.graphscope.parallel.netty;

import static org.apache.giraph.conf.GiraphConstants.MAX_IPC_PORT_BIND_ATTEMPTS;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;

import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.handler.NettyServerHandler;
import com.alibaba.graphscope.parallel.netty.request.serialization.WritableRequestDecoder;
import com.alibaba.graphscope.parallel.utils.NetworkMap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.ImmediateEventExecutor;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyServer<OID_T extends WritableComparable, GS_VID_T> {

    private static Logger logger = LoggerFactory.getLogger(NettyServer.class);
    private static AtomicInteger decoderId = new AtomicInteger(0);
    /**
     * Send buffer size
     */
    private final int sendBufferSize;
    /**
     * Receive buffer size
     */
    private final int receiveBufferSize;

    private final int maxFrameLength;
    /**
     * TCP backlog
     */
    private final int tcpBacklog;
    /**
     * Boss eventloop group
     */
    private final EventLoopGroup bossGroup;
    /**
     * Worker eventloop group
     */
    private final EventLoopGroup workerGroup;
    /**
     * Accepted channels
     */
    private final ChannelGroup accepted = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);

    ServerBootstrap bootstrap;
    private int workerId;
    private int bossThreadSize;
    private int workerThreadSize;
    private NetworkMap networkMap;
    private InetSocketAddress myAddress;
    private ImmutableClassesGiraphConfiguration conf;
    private MessageStore<OID_T, Writable, GS_VID_T> nextIncomingMessages;
    private IFragment fragment;
    //    private CopyOnWriteArrayList<NettyServerHandler<OID_T,GS_VID_T>> handlers;
    private List<NettyServerHandler<OID_T, GS_VID_T>> handlers;
    private Channel channel;

    public NettyServer(
            ImmutableClassesGiraphConfiguration conf,
            IFragment fragment,
            NetworkMap networkMap,
            MessageStore<OID_T, Writable, GS_VID_T> nextIncomingMessages,
            final UncaughtExceptionHandler exceptionHandler) {
        this.conf = conf;
        this.networkMap = networkMap;
        this.workerId = networkMap.getSelfWorkerId();
        this.nextIncomingMessages = nextIncomingMessages;
        this.fragment = fragment;
        //        handlers = new CopyOnWriteArrayList<>();
        handlers = Collections.synchronizedList(new ArrayList<>());

        bossThreadSize = GiraphConstants.NETTY_SERVER_BOSS_THREADS.get(conf);
        workerThreadSize = GiraphConstants.NETTY_SERVER_WORKER_THREADS.get(conf);
        sendBufferSize = GiraphConstants.SERVER_SEND_BUFFER_SIZE.get(conf);
        receiveBufferSize =
                GiraphConstants.SERVER_RECEIVE_BUFFER_SIZE.get(conf) + SIZE_OF_BYTE + SIZE_OF_INT;
        maxFrameLength = GiraphConstants.MAX_FRAME_LENGTH.get(conf) + SIZE_OF_BYTE + SIZE_OF_INT;

        // SO_BACKLOG controls  number of clients our server can simultaneously listen.
        tcpBacklog = conf.getWorkerNum();

        bossGroup =
                new NioEventLoopGroup(
                        bossThreadSize,
                        ThreadUtils.createThreadFactory(
                                "netty-server-boss-" + networkMap.getSelfWorkerId() + "-%d",
                                exceptionHandler));
        workerGroup =
                new NioEventLoopGroup(
                        workerThreadSize,
                        ThreadUtils.createThreadFactory(
                                "netty-server-worker-" + networkMap.getSelfWorkerId() + "-%d",
                                exceptionHandler));
    }

    public void startServer() {
        bootstrap = new ServerBootstrap();
        bootstrap
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, tcpBacklog)
                .option(ChannelOption.ALLOCATOR, conf.getNettyAllocator())
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, sendBufferSize)
                .childOption(ChannelOption.SO_RCVBUF, receiveBufferSize)
                .childOption(ChannelOption.ALLOCATOR, conf.getNettyAllocator())
                .childOption(
                        ChannelOption.RCVBUF_ALLOCATOR,
                        new AdaptiveRecvByteBufAllocator(
                                receiveBufferSize / 4, receiveBufferSize, receiveBufferSize))
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
                                p.addLast(
                                        "requestFrameDecoder",
                                        new LengthFieldBasedFrameDecoder(
                                                maxFrameLength, 0, 4, 0, 4));
                                p.addLast("requestDecoder", getDecoder(conf));
                                p.addLast("handler", getHandler());
                            }
                        });
        bindAddress();
    }

    private synchronized WritableRequestDecoder getDecoder(
            ImmutableClassesGiraphConfiguration conf) {
        return new WritableRequestDecoder(conf, decoderId.getAndAdd(1));
    }

    private synchronized NettyServerHandler<OID_T, GS_VID_T> getHandler() {
        NettyServerHandler<OID_T, GS_VID_T> handler =
                new NettyServerHandler<OID_T, GS_VID_T>(fragment, nextIncomingMessages);
        handlers.add(handler);
        logger.info("creating handler: " + handler + " current size: " + handlers.size());
        return handler;
    }

    private void bindAddress() {
        int myPort = networkMap.getSelfPort();
        String myHostNameOrIp = networkMap.getSelfHostNameOrIp();
        int maxAttempts = MAX_IPC_PORT_BIND_ATTEMPTS.get(conf);
        int curAttempt = 0;
        while (curAttempt < maxAttempts) {
            logger.info(
                    "NettyServer[{}]: try binding port {} for {}/{} times",
                    workerId,
                    myPort,
                    curAttempt,
                    maxAttempts);
            try {
                this.myAddress = new InetSocketAddress(myHostNameOrIp, myPort);
                ChannelFuture f = bootstrap.bind(myAddress).sync();

                accepted.add(f.channel());
                break;
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            } catch (Exception e) {
                // CHECKSTYLE: resume IllegalCatchCheck
                logger.warn(
                        "start: Likely failed to bind on attempt "
                                + curAttempt
                                + " to port "
                                + myPort
                                + e.getCause().toString());
                ++curAttempt;
            }
        }
        logger.info(
                "NettyServer[{}]: start: Started server [{}] with up to [{}] threads after bind"
                        + " attempts [{}], sendBufferSize = {}, receiveBufferSize = {}",
                workerId,
                myAddress,
                workerThreadSize,
                curAttempt,
                sendBufferSize,
                receiveBufferSize);
    }

    public void preSuperStep(MessageStore<OID_T, Writable, GS_VID_T> nextIncomingMessages) {
        logger.info(
                "NettyServer[{}]: Pre super step for handlers of size: {}, {}",
                workerId,
                handlers.size(),
                handlers);
        for (int i = 0; i < handlers.size(); ++i) {
            handlers.get(i).preSuperStep(nextIncomingMessages);
        }
    }

    public long getNumberOfByteReceived() {
        long cnt = 0;
        for (int i = 0; i < handlers.size(); ++i) {
            cnt += handlers.get(i).getNumberBytesReceived();
        }
        return cnt;
    }

    public void resetBytesCounter() {
        for (int i = 0; i < handlers.size(); ++i) {
            handlers.get(i).resetBytesCounter();
        }
    }

    public void close() {
        try {
            logger.debug(
                    "NettyServer [{}]: Closing channels of size {} ", workerId, accepted.size());
            accepted.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        logger.info("NettyServer [{}]: Successfully close server {}", workerId, myAddress);
    }
}
