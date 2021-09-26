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
package org.apache.giraph.comm.netty.handler;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Promise;

import org.apache.giraph.comm.requests.NettyMessage;
import org.apache.giraph.graph.AggregatorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

/**
 * Handles a client-side channel.
 */
public class NettyClientHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private static Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);
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
    private BlockingQueue<Promise<NettyMessage>> messageList =
            new ArrayBlockingQueue<Promise<NettyMessage>>(10);
    private int id;

    public NettyClientHandler(AggregatorManager aggregatorManager, int id) {
        this.aggregatorManager = aggregatorManager;
        this.id = id;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        synchronized (this) {
            Promise<NettyMessage> prom;
            while ((prom = messageList.poll()) != null) {
                prom.setFailure(new IOException("Connection lost"));
            }
            messageList = null;
        }
        logger.info("channelClosed: Closed the channel on " + ctx.channel().remoteAddress());

        ctx.fireChannelInactive();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        // Server is supposed to send nothing, but if it sends something, discard it.
        logger.info("Client [" + id + "] receive msg from server");
        synchronized (this) {
            if (messageList != null) {
                messageList.poll().setSuccess(msg);
            } else {
                throw new IllegalStateException("message list null");
            }
        }
    }

    public Future<NettyMessage> sendMessage(NettyMessage request) {
        if (ctx == null) {
            throw new IllegalStateException("ctx empty");
        }
        return sendMessage(request, ctx.executor().newPromise());
    }

    public Future<NettyMessage> sendMessage(NettyMessage request, Promise<NettyMessage> promise) {
        synchronized (this) {
            if (messageList == null) {
                // connection closed
                promise.setFailure(new IllegalStateException());
            } else if (messageList.offer(promise)) {
                logger.info("client [" + id + "] send msg:" + request + ", " + promise.toString());
                ctx.writeAndFlush(request)
                        .addListener(
                                new ChannelFutureListener() {
                                    @Override
                                    public void operationComplete(ChannelFuture future)
                                            throws Exception {
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
            } else {
                // message rejected.
                promise.setFailure(new BufferOverflowException());
            }
            return promise;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
