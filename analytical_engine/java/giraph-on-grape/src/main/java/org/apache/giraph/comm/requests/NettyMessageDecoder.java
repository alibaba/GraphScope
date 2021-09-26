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
     *     to
     * @param in the {@link ByteBuf} from which to read data
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
