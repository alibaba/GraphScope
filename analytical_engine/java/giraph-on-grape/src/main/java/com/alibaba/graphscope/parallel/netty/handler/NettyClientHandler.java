package com.alibaba.graphscope.parallel.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * handling Response.
 */
public class NettyClientHandler extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);
    private AtomicInteger messageReceivedCount;
    private int pendingRequestSize;
    private int workerId;

    public NettyClientHandler(int workerId) {
        messageReceivedCount = new AtomicInteger(0);
        this.workerId = workerId;
        pendingRequestSize = Integer.MAX_VALUE;
    }

    public AtomicInteger getMessageReceivedCount() {
        return messageReceivedCount;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf)) {
            throw new IllegalStateException("channelRead: Got a " +
                "non-ByteBuf message " + msg);
        }
        ByteBuf buf = (ByteBuf) msg;
        if (buf.readableBytes() < 4) {
            throw new IllegalStateException("Expect at least 4 bytes response");
        }
        int seq = buf.readInt();
        int cnt = messageReceivedCount.addAndGet(1);
        logger.debug("Client handler [{}] receive msg no.{} from server, current msg count {}",
            workerId, seq, cnt);
        if (cnt >= pendingRequestSize) {
            logger.debug(
                "Client handler [{}] finish waiting. since current num response arrived: {}/{}",
                workerId, cnt, pendingRequestSize);
            synchronized (messageReceivedCount) {
                messageReceivedCount.notify();
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Client handler [{}] :channelClosed: Closed the channel on {}", workerId,
                ctx.channel().remoteAddress());
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
        logger.warn(
            "Client handler [{}]: exceptionCaught: Channel channelId={}, failed with remote address {}, cause: {}",
            workerId, ctx.channel().hashCode(),
            ctx.channel().remoteAddress(), cause);
    }

    public void postSuperStep() {
        this.pendingRequestSize = Integer.MAX_VALUE;
        messageReceivedCount.set(0);
    }

    public void waitForResponse(int pendingRequestSize) {
        this.pendingRequestSize = pendingRequestSize;
        logger.debug("Client handler [{}] waiting for responses {}", workerId, pendingRequestSize);
        if (messageReceivedCount.get() == pendingRequestSize) {
            if (pendingRequestSize == 0) {
                logger.debug("no waiting since no message sent");
                return;
            }
            logger.debug("Client handler [{}] All responses have arrived before starting waiting.", workerId);
            return;
        } else if (messageReceivedCount.get() > pendingRequestSize) {
            throw new IllegalStateException("Not possible");
        }
        synchronized (messageReceivedCount) {
            try {
                logger.debug("Client handler [{}] starting waiting for response, {}/{}", workerId, messageReceivedCount.get(), pendingRequestSize);
                messageReceivedCount.wait();
                logger.debug("Client handler [{}] finish waiting for response, {}/{}", workerId, messageReceivedCount.get(), pendingRequestSize);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

//public class NettyClientHandler extends SimpleChannelInboundHandler<WritableRequest> {
//    private static Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);
//
//    /**
//     * <strong>Please keep in mind that this method will be renamed to
//     * {@code messageReceived(ChannelHandlerContext, I)} in 5.0.</strong>
//     * <p>
//     * Is called for each message of type {@link }.
//     *
//     * @param ctx the {@link ChannelHandlerContext} which this {@link SimpleChannelInboundHandler}
//     *            belongs to
//     * @param msg the message to handle
//     * @throws Exception is thrown if an error occurred
//     */
//    @Override
//    protected void channelRead0(ChannelHandlerContext ctx, WritableRequest msg) throws Exception {
//        logger.info("Client doesn't expect message from server");
//    }
//
//    @Override
//    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//        super.channelInactive(ctx);
//        logger.info("channelClosed: Closed the channel on " + ctx.channel().remoteAddress());
//        ctx.fireChannelInactive();
//    }
//
//    @Override
//    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
//        // Close the connection when an exception is raised.
//        cause.printStackTrace();
//        ctx.close();
//    }
//}
