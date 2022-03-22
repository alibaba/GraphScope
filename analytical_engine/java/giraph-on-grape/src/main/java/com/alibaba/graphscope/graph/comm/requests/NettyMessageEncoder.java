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
package com.alibaba.graphscope.graph.comm.requests;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encode a netty message obj into a bytebuffer.
 */
public class NettyMessageEncoder extends MessageToByteEncoder {

    private static Logger logger = LoggerFactory.getLogger(NettyMessageEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        if (!(msg instanceof NettyMessage)) {
            throw new IllegalArgumentException("encode: Got a message of type " + msg.getClass());
        }
        NettyMessage request = (NettyMessage) msg;
        int requestSize = request.getSerializedSize();
        requestSize += SIZE_OF_BYTE;
        if (requestSize <= 0) {
            logger.error("Request size less than zero.");
            return;
        }
        out.capacity(requestSize);

        // This will later be filled with the correct size of serialized request
        out.writeByte(request.getMessageType().ordinal());
        ByteBufOutputStream output = new ByteBufOutputStream(out);
        try {
            request.write(output);
        } catch (IndexOutOfBoundsException e) {
            logger.error(
                    "write: Most likely the size of request was not properly "
                            + "specified (this buffer is too small) - see getSerializedSize() "
                            + "in "
                            + request.getMessageType().getRequestClass());
            throw new IllegalStateException(e);
        }
        output.flush();
        output.close();

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "write: Client "
                            + ", size = "
                            + out.readableBytes()
                            + ", "
                            + request.getMessageType()
                            + " took ");
        }
        logger.info("Encode msg: " + String.join("-", out.toString()));
    }
}
