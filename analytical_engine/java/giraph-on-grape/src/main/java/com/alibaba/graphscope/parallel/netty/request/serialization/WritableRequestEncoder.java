package com.alibaba.graphscope.parallel.netty.request.serialization;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;

import com.alibaba.graphscope.parallel.netty.request.WritableRequest;
import com.alibaba.graphscope.parallel.netty.request.impl.ByteBufRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableRequestEncoder extends ChannelOutboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(WritableRequestEncoder.class);
    /**
     * Buffer starting size
     */
    private final int bufferStartingSize;

    public WritableRequestEncoder(ImmutableClassesGiraphConfiguration conf) {
        bufferStartingSize =
            GiraphConstants.NETTY_REQUEST_ENCODER_BUFFER_SIZE.get(conf) + SIZE_OF_INT
                + SIZE_OF_BYTE;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg,
        ChannelPromise promise) throws Exception {
        if (logger.isDebugEnabled()){
            logger.debug("Enter encoder");
        }
        ByteBuf buf;
        if (msg instanceof WritableRequest) {
            WritableRequest request = (WritableRequest) msg;
            int requestSize = request.getNumBytes();

            if (msg instanceof ByteBufRequest) {
                ByteBufRequest bufRequest = (ByteBufRequest) request;
                buf = bufRequest.getBuffer();
                if ((buf.readableBytes() - SIZE_OF_BYTE - SIZE_OF_INT) % 16 != 0) {
                    logger.error("Wrong number of bytes: {}", buf.readableBytes());
                    //FIXME: this exception not catched.
                    throw new IllegalStateException(
                        "Wrong number of bytes: " + buf.readableBytes());
                }


                buf.setInt(0, buf.readableBytes() - SIZE_OF_INT);
                buf.setByte(4, bufRequest.getRequestType().ordinal());

                if (logger.isDebugEnabled()) {
                    logger.debug("encode ByteBufRequest length: {}, type {}",
                        buf.readableBytes() - SIZE_OF_INT,
                        bufRequest.getRequestType().getClazz().getName());
                }
//                buf.retain();
                ctx.writeAndFlush(buf,
                    promise); // can be released
//                promise.await();
            } else {
                if (requestSize == WritableRequest.UNKNOWN_SIZE) {
                    logger.debug("Unknown size of request, using default size: {}",
                        bufferStartingSize);
                    buf = ctx.alloc().buffer(bufferStartingSize);
                } else {
                    requestSize += SIZE_OF_BYTE + SIZE_OF_INT;
                    buf = ctx.alloc().buffer(requestSize);
                }
                ByteBufOutputStream output = new ByteBufOutputStream(buf);
                output.writeInt(0);
                output.writeByte(request.getRequestType().ordinal());
                try {
                    request.write(output);
                } catch (IndexOutOfBoundsException e) {
                    logger.error("write: Most likely the size of request was not properly " +
                        "specified (this buffer is too small) - see getSerializedSize() " +
                        "in " + request.getRequestType().getClazz());
                    throw new IllegalStateException(e);
                }
                output.flush();
                output.close();

                buf.setInt(0, buf.writerIndex() - SIZE_OF_INT);
                if (logger.isDebugEnabled()){
                    logger.debug("Encode msg, type: [{}], writen bytes: [{}]",
                        request.getRequestType().getClazz().getName(), buf.readableBytes());
                }
                ctx.writeAndFlush(buf, promise);
            }
        } else {
            logger.error("Encoder: got instance " + msg + ", expect a WritableRequest");
        }
    }
}

//    /**
//     * Encode a message into a {@link ByteBuf}. This method will be called for each written message
//     * that can be handled by this encoder.
//     *
//     * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs
//     *            to
//     * @param msg the message to encode
//     * @param out the {@link ByteBuf} into which the encoded message will be written
//     * @throws Exception is thrown if an error occurs
//     */
//    @Override
//    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
//        if (msg instanceof WritableRequest){
//            WritableRequest request = (WritableRequest) msg;
//            //at least
//            int requestSize = request.getNumBytes();
//            if (requestSize == WritableRequest.UNKNOWN_SIZE){
//                logger.debug("Unknown size of request, using default size: {}", bufferStartingSize);
//                out.capacity(bufferStartingSize);
//            }
//            else {
//                out.capacity(requestSize + SIZE_OF_BYTE + SIZE_OF_INT);
//            }
//            ByteBufOutputStream output = new ByteBufOutputStream(out);
//            //Currently we don't know how many bytes written, revisit this later
//            output.writeInt(0);
//            output.writeByte(request.getRequestType().ordinal());
//            try {
//                request.write(output);
//            } catch (IndexOutOfBoundsException e) {
//                logger.error("write: Most likely the size of request was not properly " +
//                    "specified (this buffer is too small) - see getSerializedSize() " +
//                    "in " + request.getRequestType().getClazz());
//                throw new IllegalStateException(e);
//            }
//            output.flush();
//            output.close();
//
//            out.setInt(0, out.writerIndex() - SIZE_OF_INT);
//            logger.info("Encode msg, type: " + request.getRequestType().getClazz().getName() + ", writen bytes: " + out.readableBytes());
//        }
//        else {
//            logger.error("Encoder: got instance " + msg + ", expect a WritableRequest");
//        }
//    }
//}
