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
import com.alibaba.graphscope.graph.comm.requests.NettyWritableMessage;
import com.alibaba.graphscope.graph.impl.AggregatorManagerNettyImpl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles a server-side channel.
 */
public class ServerHandler extends SimpleChannelInboundHandler<Object> {

    private static Logger logger = LoggerFactory.getLogger(ServerHandler.class);
    private AggregatorManagerNettyImpl aggregatorManager;
    // private Map<String, Integer> aggregateTimes;
    private ByteBufAllocator allocator;
    private ByteBuf buffer;
    private AtomicInteger msgNo;

    public ServerHandler(AggregatorManager aggregatorManager, AtomicInteger msgNo) {
        logger.info("Creating server hanlder in thread: " + Thread.currentThread().getName());
        this.aggregatorManager = (AggregatorManagerNettyImpl) aggregatorManager;
        this.allocator = new PooledByteBufAllocator();
        this.buffer = allocator.buffer();
        this.msgNo = msgNo;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof NettyMessage) {
            int no = msgNo.addAndGet(1);
            logger.info(
                    "server thread id "
                            + Thread.currentThread().getId()
                            + "receiving: msg no."
                            + no);
            NettyMessage message = (NettyMessage) msg;
            if (message instanceof NettyWritableMessage) {
                NettyWritableMessage aggregatorMessage = (NettyWritableMessage) message;

                aggregatorManager.acceptNettyMessage(aggregatorMessage);
                String aggregatorId = aggregatorMessage.getId();
                logger.info(
                        "server thread: ["
                                + Thread.currentThread().getId()
                                + "]: aggregating id: "
                                + aggregatorMessage.getId()
                                + " counts: "
                                + no
                                + ", need: "
                                + (aggregatorManager.getNumWorkers() - 1));

                NettyWritableMessage toSend = null;
                if ((no % (aggregatorManager.getNumWorkers() - 1) == 0)) {
                    logger.info(
                            "Server ["
                                    + Thread.currentThread().getId()
                                    + "] Received last msg for agg: "
                                    + aggregatorId
                                    + " notify on "
                                    + this.msgNo);
                    synchronized (this.msgNo) {
                        this.msgNo.notifyAll();
                    }
                } else {
                    synchronized (this.msgNo) {
                        try {
                            if (this.msgNo.get() % (aggregatorManager.getNumWorkers() - 1) != 0) {
                                logger.info(
                                        "Server "
                                                + Thread.currentThread().getId()
                                                + " wait on msgNo: "
                                                + this.msgNo);
                                this.msgNo.wait();
                            } else {
                                logger.info(
                                        "when server hanlder "
                                                + Thread.currentThread().getId()
                                                + " try to wait, find alread satisfied: "
                                                + this.msgNo.get());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        logger.info(
                                "server "
                                        + Thread.currentThread().getId()
                                        + "finish waiting: "
                                        + this.msgNo);
                    }
                }
                Writable writable = aggregatorManager.getAggregatedValue(aggregatorId);
                toSend = new NettyWritableMessage(writable, 1000000, aggregatorId);
                logger.info(
                        "server ["
                                + Thread.currentThread().getId()
                                + "] send response to client: "
                                + toSend);

                ctx.writeAndFlush(toSend);

            } else {
                logger.error("Not a aggregator message");
            }
        } else {
            logger.error("Expect a netty message.");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
