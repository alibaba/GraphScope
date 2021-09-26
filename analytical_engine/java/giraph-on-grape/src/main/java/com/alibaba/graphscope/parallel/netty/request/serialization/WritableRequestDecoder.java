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
package com.alibaba.graphscope.parallel.netty.request.serialization;

import com.alibaba.graphscope.parallel.netty.request.RequestType;
import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import com.alibaba.graphscope.parallel.netty.request.impl.ByteBufRequest;

import com.alibaba.graphscope.parallel.netty.request.impl.ByteBufRequest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableRequestDecoder extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(WritableRequestDecoder.class);

    private ImmutableClassesGiraphConfiguration conf;
    private int waitingFullMsgTimes;
    private int decoderId;

    public WritableRequestDecoder(ImmutableClassesGiraphConfiguration conf, int decoderId) {
        this.conf = conf;
        waitingFullMsgTimes = 0;
        this.decoderId = decoderId;
    }

    /**
     * Reade the byteBuffer(Obtained from lengthFieldDecoder), forward the request to the next
     * handler.
     *
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ByteBuf)) {
            throw new IllegalStateException("decode: Got illegal message " + msg);
        }
        // Decode the request type
        ByteBuf buf = (ByteBuf) msg;
        int enumValue = buf.readByte();
        RequestType type = RequestType.values()[enumValue];
        Class<? extends WritableRequest> messageClass = type.getClazz();
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Decoder {}-{}: message clz: {}, bytes to read {}",
                    conf.getWorkerId(),
                    decoderId,
                    messageClass.getName(),
                    buf.readableBytes());
        }

        WritableRequest request = ReflectionUtils.newInstance(messageClass);
        // Conf contains class info to create message instance.
        request.setConf(conf);
        if (request.getRequestType().getClazz().equals(ByteBufRequest.class)) {
            request.setBuffer(buf);
        } else {
            ByteBufInputStream input = new ByteBufInputStream(buf);
            request.readFields(input);
        }
        buf.retain();
        //        buf.release();
        if (logger.isDebugEnabled()) {
            logger.debug("decode res: {}", request);
        }
        ctx.fireChannelRead(request);
    }
}
