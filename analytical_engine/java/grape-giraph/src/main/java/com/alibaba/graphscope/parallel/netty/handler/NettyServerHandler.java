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
package com.alibaba.graphscope.parallel.netty.handler;

import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.message.MessageStore;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServerHandler<OID_T extends WritableComparable, GS_VID_T>
        extends SimpleChannelInboundHandler<WritableRequest> {

    public static int RESPONSE_BYTES = 4;
    private static Logger logger = LoggerFactory.getLogger(NettyServerHandler.class);

    private MessageStore<OID_T, Writable, GS_VID_T> nextIncomingMessages;
    private IFragment<?, GS_VID_T, ?, ?> fragment;
    private int msgSeq;
    private long byteCounter;

    public NettyServerHandler(
            IFragment<?, GS_VID_T, ?, ?> fragment,
            MessageStore<OID_T, Writable, GS_VID_T> nextIncomingMessages) {
        this.fragment = fragment;
        this.nextIncomingMessages = nextIncomingMessages;
        this.msgSeq = 0;
        this.byteCounter = 0;
    }

    /**
     * <strong>Please keep in mind that this method will be renamed to {@code
     * messageReceived(ChannelHandlerContext, I)} in 5.0.</strong>
     *
     * <p>Is called for each message of type {@link WritableRequest}.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link SimpleChannelInboundHandler}
     *            belongs to
     * @param msg the message to handle
     * @throws Exception is thrown if an error occurred
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WritableRequest msg) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Server handler [{}] thread: [{}] received msg: {}",
                    fragment.fid(),
                    Thread.currentThread().getId(),
                    msg);
        }
        byteCounter += msg.getBuffer().readableBytes();
        //        msg.doRequest(nextIncomingMessages);
        // dealloc the buffer here.
        msg.getBuffer().release(2);

        ByteBuf buf = ctx.alloc().buffer(RESPONSE_BYTES);
        buf.writeInt(msgSeq);
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Server handler[{}] thread [{}] send response [{}]",
                    fragment.fid(),
                    Thread.currentThread().getId(),
                    msgSeq);
        }
        ctx.writeAndFlush(buf);
        msgSeq += 1;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }

    public void preSuperStep(MessageStore<OID_T, Writable, GS_VID_T> nextIncomingMessages) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Update nextIncoming msg store from "
                            + this.nextIncomingMessages
                            + " to "
                            + nextIncomingMessages);
        }
        this.nextIncomingMessages = nextIncomingMessages;
        this.msgSeq = 0;
    }

    public long getNumberBytesReceived() {
        return byteCounter;
    }

    public void resetBytesCounter() {
        byteCounter = 0;
    }
}
