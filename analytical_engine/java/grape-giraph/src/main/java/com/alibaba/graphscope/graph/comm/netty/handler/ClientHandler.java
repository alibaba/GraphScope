/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.graph.comm.netty.handler;

import com.alibaba.graphscope.graph.AggregatorManager;
import com.alibaba.graphscope.graph.comm.requests.NettyMessage;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Promise;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

/**
 * Handles a client-side channel.
 */
public class ClientHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private static Logger logger = LoggerFactory.getLogger(ClientHandler.class);
    private final ChannelFutureListener trafficGenerator =
            new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (future.isSuccess()) {
                        // generateTraffic();
                        logger.info("successfully send msg times: ");
                    } else {
                        future.cause().printStackTrace();
                        future.channel().close();
                    }
                }
            };
    //    private ByteBuf content;
    private ChannelHandlerContext ctx;
    private AggregatorManager aggregatorManager;
    private BlockingQueue<NettyMessage> messageList = new ArrayBlockingQueue<NettyMessage>(10);
    private int id;

    public ClientHandler(AggregatorManager aggregatorManager, int id) {
        this.aggregatorManager = aggregatorManager;
        this.id = id;
    }

    public NettyMessage getResponse() {
        boolean interrupted = false;
        try {
            for (; ; ) {
                try {
                    return messageList.take();
                } catch (InterruptedException ignore) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        logger.info("channelClosed: Closed the channel on " + ctx.channel().remoteAddress());

        ctx.fireChannelInactive();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        boolean res = messageList.offer(msg);
        logger.info("Client [" + id + "] received msg from server, " + msg);
        assert res;
    }

    // Do not use the promise!
    public Future<NettyMessage> sendMessage(NettyMessage request) {
        if (ctx == null) {
            throw new IllegalStateException("ctx empty");
        }
        return sendMessage(request, ctx.executor().newPromise());
    }

    public Future<NettyMessage> sendMessage(NettyMessage request, Promise<NettyMessage> promise) {
        logger.info("client [" + id + "] send msg:" + request + ", " + promise.toString());
        ctx.writeAndFlush(request)
                .addListener(
                        new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (future.isSuccess()) {
                                    logger.info("client +[ " + id + "] finish sending");
                                    // aggregatorManager.notify();
                                    //
                                    // promise.notifyAll();
                                } else {
                                    future.cause().printStackTrace();
                                    future.channel().close();
                                }
                            }
                        });
        return promise;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
