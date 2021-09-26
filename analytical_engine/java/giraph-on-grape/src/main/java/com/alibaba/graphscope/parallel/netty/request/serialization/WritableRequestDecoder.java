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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.RequestUtils;
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
        request = RequestUtils.decodeWritableRequest(buf, request);
        buf.retain();
        //        buf.release();
        if (logger.isDebugEnabled()) {
            logger.debug("decode res: {}", request);
        }
        ctx.fireChannelRead(request);
    }

    //    /**
    //     * Decode the from one {@link ByteBuf} to an other. This method will be called till either
    // the
    //     * input {@link ByteBuf} has nothing to read when return from this method or till nothing
    // was
    //     * read from the input {@link ByteBuf}.
    //     *
    //     * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder}
    // belongs
    //     *            to
    //     * @param in  the {@link ByteBuf} from which to read data
    //     * @param out the {@link List} to which decoded messages should be added
    //     * @throws Exception is thrown if an error occurs
    //     */
    //    @Override
    //    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    //        throws Exception {
    //        if (in.readableBytes() < 4 + 1) {
    //            logger.warn("return since only "+ in.readableBytes() + " available, need at least
    // 5");
    //            return;
    //        }
    //        in.markReaderIndex();
    //        //num of bytes
    //        int numBytes = in.readInt();
    //        if (numBytes < 0){
    //            logger.error("Expect a positive number of bytes" + numBytes);
    ////            ctx.close();
    //            return ;
    //        }
    //        if (in.readableBytes() < numBytes){
    //            in.resetReaderIndex();
    //            logger.error("Decoder [" + conf.getWorkerId() + "-" + decoderId + "] not enough
    // num bytes: " + in.readableBytes() + " expected: " + numBytes + " times:" +
    // waitingFullMsgTimes++);
    //            return ;
    //        }
    //        logger.warn("Setting waiting full msg times from :" + waitingFullMsgTimes + " to 0");
    //        waitingFullMsgTimes = 0;
    //
    //        // Decode the request type
    //        int enumValue = in.readByte();
    //        RequestType type = RequestType.values()[enumValue];
    //        Class<? extends WritableRequest> messageClass = type.getClazz();
    //
    //        logger.debug(
    //                "decode: Client "
    //                    + messageClass.getName()
    //                    + ", with size "
    //                    + in.readableBytes());
    //
    //        WritableRequest request = ReflectionUtils.newInstance(messageClass);
    //        //Conf contains class info to create message instance.
    //        request.setConf(conf);
    //        request = RequestUtils.decodeWritableRequest(in, request);
    //        assert in.release();
    //        logger.debug("decode res: " + request);
    //        out.add(request);
    //    }
}
