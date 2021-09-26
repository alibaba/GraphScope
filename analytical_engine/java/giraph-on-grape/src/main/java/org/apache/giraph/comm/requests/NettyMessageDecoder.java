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
package org.apache.giraph.comm.requests;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.RequestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class NettyMessageDecoder extends ByteToMessageDecoder {

    private static Logger logger = LoggerFactory.getLogger(NettyMessageDecoder.class);

    /**
     * Decode the from one {@link ByteBuf} to an other. This method will be called till either the
     * input {@link ByteBuf} has nothing to read when return from this method or till nothing was
     * read from the input {@link ByteBuf}.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs
     *            to
     * @param in  the {@link ByteBuf} from which to read data
     * @param out the {@link List} to which decoded messages should be added
     * @throws Exception is thrown if an error occurs
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {
        if (in.readableBytes() < 8000000) {
            return;
        }
        in.markReaderIndex();

        // Decode the request
        int enumValue = in.readByte();
        logger.info("ByteBuf direct or not: " + in.isDirect());
        NettyMessageType type = NettyMessageType.values()[enumValue];
        Class<? extends NettyMessage> messageClass = type.getRequestClass();

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "decode: Client "
                            + messageClass.getName()
                            + ", with size "
                            + in.readableBytes());
        }
        NettyMessage message = ReflectionUtils.newInstance(messageClass);
        message = RequestUtils.decodeNettyMessage(in, message);

        out.add(message);
    }
}
