package org.apache.giraph.comm.requests;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encode a netty message obj into a bytebuffer.
 */
public class NettyMessageEncoder extends MessageToByteEncoder {
    private static Logger logger = LoggerFactory.getLogger(NettyMessageEncoder.class);

    /**
     * Encode a message into a {@link ByteBuf}. This method will be called for each written message
     * that can be handled by this encoder.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs
     *            to
     * @param msg the message to encode
     * @param out the {@link ByteBuf} into which the encoded message will be written
     * @throws Exception is thrown if an error occurs
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        if (!(msg instanceof NettyMessage)) {
            throw new IllegalArgumentException(
                "encode: Got a message of type " + msg.getClass());
        }
        NettyMessage request = (NettyMessage) msg;
        int requestSize = request.getSerializedSize();
        requestSize += SIZE_OF_BYTE;
        if (requestSize <= 0){
            logger.error("Request size less than zero.");
            return ;
        }
        out.capacity(requestSize);

        // This will later be filled with the correct size of serialized request
        out.writeByte(request.getMessageType().ordinal());
        ByteBufOutputStream output = new ByteBufOutputStream(out);
        try {
            request.write(output);
        } catch (IndexOutOfBoundsException e) {
            logger.error("write: Most likely the size of request was not properly " +
                "specified (this buffer is too small) - see getSerializedSize() " +
                "in " + request.getMessageType().getRequestClass());
            throw new IllegalStateException(e);
        }
        output.flush();
        output.close();

        if (logger.isDebugEnabled()) {
            logger.debug("write: Client " +
                ", size = " + out.readableBytes() + ", " +
                request.getMessageType() + " took ");
        }
        logger.info("Encode msg: " + String.join("-", out.toString()));
    }

}
