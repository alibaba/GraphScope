package com.alibaba.graphscope.example.message;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.ds.ProjectedNbr;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.example.circle.parallel.formal.CircleAppParallel;
import com.alibaba.graphscope.example.circle.parallel.formal.CircleUtil;
import com.alibaba.graphscope.example.circle.parallel.formal.PathSerAndDeser;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.MessageInBuffer;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.fastffi.CXXValueScope;
import com.carrotsearch.hppc.LongArrayList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.graphscope.parallel.message.DoubleMsg;

public class Message implements ParallelAppBase<Long, Long, Long, Long, MessageContext> {

    private static final Logger logger = LoggerFactory.getLogger(Message.class);
    private List<LongArrayList> msgs = createMessages();
    // private DoubleMsg doubleMsg = FFITypeFactoryhelper.newDoubleMsg(1.0);

    @Override
    public void PEval(IFragment<Long, Long, Long, Long> iFragment,
        ParallelContextBase<Long, Long, Long, Long> parallelContextBase,
        ParallelMessageManager parallelMessageManager) {
        MessageContext ctx = (MessageContext) parallelContextBase;
        logger.info("Frag id: " + iFragment.fid() + " PEval");
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (long i = 0; i < iFragment.getInnerVerticesNum(); ++i) {
            // 表示获取 到 frag 内部的第 几个 点
            vertex.setValue(i);
            try {
                // 内存处理
                sendToAdjList(iFragment,ctx, parallelMessageManager, vertex, 0, false);
            } catch (IOException e) {
                logger.error("PEval error", e);
            }
        }
        vertex.delete();

        parallelMessageManager.forceContinue();
        ctx.currentStep += 1;
    }

    @Override
    public void IncEval(IFragment<Long, Long, Long, Long> iFragment,
        ParallelContextBase<Long, Long, Long, Long> parallelContextBase,
        ParallelMessageManager parallelMessageManager) {
        MessageContext ctx = (MessageContext) parallelContextBase;

        if (ctx.currentStep >= ctx.maxSteps) {
            return;
        }

        // receive messages
        receiveMessages(iFragment, ctx, parallelMessageManager, false);

        // sendToNbr(iFragment, parallelMessageManager, vertex, 0, 0);
        parallelSendToNbr(iFragment, parallelMessageManager, ctx);
        ctx.currentStep += 1;
    }

    void receiveMessages(IFragment<Long, Long, Long, Long> frag, MessageContext ctx,
        ParallelMessageManager messageManager, boolean simple) {
        long start = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch(ctx.threadNum);
        MessageInBuffer.Factory bufferFactory = FFITypeFactoryhelper.newMessageInBuffer();
        for (int tid = 0; tid < ctx.threadNum; ++tid) {
            final int finalTid = tid;
            ctx.executor.execute(new Runnable() {
                @Override
                public void run() {
                    // 每个线程 维护一个 messageInBuffer
                    MessageInBuffer messageInBuffer = bufferFactory.create();
                    boolean result;
                    while (true) {
                        result = messageManager.getMessageInBuffer(messageInBuffer);
                        if (result) {
                            if (!simple){
                                try {
                                    receiveMessageImpl(frag, messageInBuffer, simple);
                                } catch (Exception e) {
                                    logger.error(
                                        "Error when receiving message in fragment {} thread {}",
                                        frag.fid(), finalTid, e);
                                }
                            }
                        } else {
                            break;
                        }
                    }
                    messageInBuffer.delete();
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            logger.error("receiveMessageAndUpdateVertex error.", e);
            ctx.executor.shutdown();
        }
    }

    void receiveMessageImpl(IFragment<Long, Long, Long, Long> frag,
        MessageInBuffer buffer, boolean simple) throws IOException {
        FFIByteVector tmpVector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        Vertex<Long> tmpVertex = FFITypeFactoryhelper.newVertexLong();

        List<LongArrayList> receivedMsg = new ArrayList<>();
        if (simple){
            return ; // do nothing
        }
        else {
            while (buffer.getPureMessage(tmpVector)) {
                tmpVector.touch();
                FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(tmpVector);
                long gid = inputStream.readLong();
                if (!frag.innerVertexGid2Vertex(gid, tmpVertex)) {
                    logger.error("Fail to get lid from gid {}", gid);
                }
                int size = inputStream.readInt();

                if (size != 0) {
                    for (int i = 0; i < size; i++) {
                        LongArrayList path = PathSerAndDeser.deserialize(inputStream);
                        receivedMsg.add(path);
                    }
                }
            }
        }
        tmpVector.delete();
        tmpVertex.delete();
        // logger.info("Received {} messages", receivedMsg.size());
        receivedMsg.clear();
    }


    void parallelSendToNbr(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager, MessageContext ctx) {
        logger.info("Send message through oe");
        CountDownLatch countDownLatch = new CountDownLatch(ctx.threadNum);
        AtomicInteger atomicInteger = new AtomicInteger(0);
        int chunkSize = 256;

        int originEnd = (int) frag.getInnerVerticesNum();
        for (int tid = 0; tid < ctx.threadNum; ++tid) {
            final int finalTid = tid;
            ctx.executor.execute(
                new Runnable() {
                    @Override
                    public void run() {
                        int cnt = 0;
                        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                        while (true) {
                            int curBegin =
                                Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                            int curEnd = Math.min(curBegin + chunkSize, originEnd);
                            if (curBegin >= originEnd) {
                                break;
                            }
                            for (long i = curBegin; i < curEnd; ++i) {
                                cnt += 1;
                                vertex.setValue(i);
                                try {
                                    sendToAdjList(frag, ctx, messageManager, vertex, finalTid, false);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        vertex.delete();
                        logger.info("Thread {} send {} vertices", finalTid, cnt);
                        countDownLatch.countDown();
                    }
                });
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
            ctx.executor.shutdown();
        }
    }

    void sendToAdjList(IFragment<Long, Long, Long, Long> frag, MessageContext ctx, ParallelMessageManager messageManager, Vertex<Long> vertex,  int threadId, boolean simple) throws IOException {
        if (simple){
            for (int j = 0; j < 100; ++j){
                messageManager.sendMsgThroughOEdges(frag, vertex,  1.0, threadId);
            }
        }
        else {
            FFIByteVectorOutputStream msgVector = ctx.getMsgVectorStream(threadId);
            AdjList<Long, Long> nbrs =  frag.getOutgoingAdjList(vertex);
            logger.info("nbr size : {}" , nbrs.size());
            
            Nbr<Long,Long> begin = nbrs.begin();
            Nbr<Long,Long> end = nbrs.end();
            logger.info("begin addr {}, end addr {}", begin.getAddress(), end.getAddress());
            while (!begin.eq(end)){
                Vertex<Long> nbrVertex = begin.neighbor();
                if (frag.isOuterVertex(nbrVertex)) {
                    for (int j = 0; j < 100; ++j){
                        msgVector.reset();
                        msgVector.writeLong(frag.getOuterVertexGid(nbrVertex));
                        msgVector.writeInt(msgs.size());
                        for (LongArrayList msg : msgs) {
                            PathSerAndDeser.serialize(msgVector, msg);
                        }
                        msgVector.finishSetting();
                        messageManager.sendToFragment(frag.getFragId(nbrVertex),msgVector.getVector(), threadId);
                    }
                }
                else {
                    // skip for send to inner vertex
                }
                begin.inc();
            }
            begin.delete();
            end.delete();


            // if (vertex.getValue() % 1000 != 0) {
            //     return ;
            // }
            // Vertex<Long> nbrVertex = FFITypeFactoryhelper.newVertexLong();
            // for (long vid = frag.getInnerVerticesNum(); vid < frag.getVerticesNum(); ++vid) {
            //     nbrVertex.setValue(vid);
            //     for (int j = 0; j < 10; ++j){
            //         msgVector.reset();
            //         msgVector.writeLong(frag.getOuterVertexGid(nbrVertex));
            //         msgVector.writeInt(msgs.size());
            //         for (LongArrayList msg : msgs) {
            //             PathSerAndDeser.serialize(msgVector, msg);
            //         }
            //         msgVector.finishSetting();
            //         messageManager.sendToFragment(frag.getFragId(nbrVertex),msgVector.getVector(), threadId);
            //     }
            // }
            // nbrVertex.delete();
        }
    }

    List<LongArrayList> createMessages() {
        List<LongArrayList> list =  new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            LongArrayList curList = new LongArrayList(10);
            for (int j = 0; j < 10; ++j) {
                curList.add(j);
            }
            list.add(curList);
        }
        return list;
    }
}
