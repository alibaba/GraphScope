package com.alibaba.graphscope.example.circle.parallel.formal.store;

import com.alibaba.graphscope.example.circle.parallel.formal.CircleAppParallelContext;
import com.alibaba.graphscope.example.circle.parallel.formal.CircleUtil;
import com.alibaba.graphscope.example.circle.parallel.formal.PathSerAndDeser;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.MessageInBuffer;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.carrotsearch.hppc.LongArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * PIE 环形抽象计算类
 * 区分Disk模式和Memory模式
 *
 * @author liumin
 * @date 2024-08-11
 */
public abstract class ComputeStep {
    private static final Logger logger = LoggerFactory.getLogger(ComputeStep.class);
    protected CircleAppParallelContext ctx;

    public ComputeStep(CircleAppParallelContext ctx) {
        this.ctx = ctx;
    }

    /**
     * 初始化
     *
     * @param frag           子图
     * @param messageManager 消息管理器
     */
    public abstract void initial(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager);

    /**
     * 迭代计算
     *
     * @param frag           子图
     * @param messageManager 消息管理器
     */
    public abstract void inc(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager);

    /**
     * frag 内部发送消息后处理步骤
     * 本机发送给 frag 内部的 inner vertex
     *
     * @param frag 子图
     * @param nbrV 邻居节点
     * @param msg  消息
     */
    public abstract void sendToLocalFrag(IFragment<Long, Long, Long, Long> frag, Vertex<Long> nbrV, List<LongArrayList> msg);

    /**
     * 接收 frag 之间的消息处理步骤
     *
     * @param frag 子图
     * @param lid  内部自增id
     * @param oid  原始id
     * @param msgs 消息列表
     */
    public abstract void processUponReceiveMsg(IFragment<Long, Long, Long, Long> frag, int lid, long oid, List<LongArrayList> msgs);

    /**
     * 获取当前点的属性信息，即保留的路径列表
     *
     * @param lid     内部自增id
     * @param batchId frag 内部 batchId
     * @return 点的属性信息
     */
    public abstract Set<LongArrayList> getVertexPath(int lid, Integer batchId);

    /**
     * 构造消息及发送消息
     *
     * @param frag           子图
     * @param messageManager 消息管理器
     * @param vertex         点
     * @param isOutGoing     消息发送方向
     * @param threadId       线程id
     * @param batchId        超步内批次ID
     * @throws IOException 抛出的异常
     */
    private void sendToAdjList(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager, Vertex<Long> vertex, boolean isOutGoing, int threadId, int batchId) throws IOException {
        // 获取点的属性信息
        Set<LongArrayList> vertexPath = getVertexPath(vertex.getValue().intValue(), batchId);
        if (vertexPath == null || vertexPath.isEmpty()) {
            return;
        }

        List<LongArrayList> msg = ctx.getPathListObject();
        FFIByteVectorOutputStream msgVector = ctx.getMsgVectorObject();

        long currOid = frag.getId(vertex);
        AdjList<Long, Long> nbrs = isOutGoing ? frag.getOutgoingAdjList(vertex) : frag.getIncomingAdjList(vertex);

        // 1. 合并多边
        Map<Long, Vertex<Long>> nbrMap = ctx.getNbrMapObject();
        mergeMutiEdgesToOne(nbrMap, frag, nbrs);

        // currentPaths can not be modified in this loop
        for (Vertex<Long> nbrV : nbrMap.values()) {
            long nbrOid = frag.getId(nbrV);

            if (currOid == nbrOid) {
                continue;
            }

            // 2. 构造要发送的消息
            if (vertexPath.size() > ctx.pathThreshold && ctx.allSeed) {
                CircleUtil.makeAndCompressMsgToSendAlongEdge(ctx.superStep, vertexPath, currOid, nbrOid, msg, ctx.isDirected);
            } else {
                CircleUtil.makeMsgToSendAlongEdge(ctx.superStep, vertexPath, currOid, nbrOid, msg, ctx.isDirected);
            }

            // 3.send msg to nbr
            sendMsgToOuterVertexOrUpdateInnerVertex(frag, messageManager, msgVector, msg, nbrV, threadId);
            if (frag.getInnerVerticesNum() < 30) {
                logger.info("makeMsgs end.superStep={},msgToSend={},src={},srcFrag={},dst={},dstFrag={},vertexCurrentPath={},circleResults={}", ctx.superStep, msg.toString(), frag.getId(vertex), frag.getFragId(vertex), nbrOid, frag.getFragId(nbrV), vertexPath, ctx.circleResult);
            }
            msg.clear();
        }
        nbrMap.clear();
    }

    /**
     * 合并两点间多边
     *
     * @param map  存储nbrV
     * @param frag 子图
     * @param nbrs Graphscope 获取的邻居列表，注意 Nbr 不能被多次使用，需要从 Nbr 中获取需要的数据
     */
    private void mergeMutiEdgesToOne(Map<Long, Vertex<Long>> map, IFragment<Long, Long, Long, Long> frag, AdjList<Long, Long> nbrs) {
        for (Nbr<Long, Long> longLongNbr : nbrs.iterable()) {
            Vertex<Long> nbrV = longLongNbr.neighbor();
            map.putIfAbsent(frag.getId(nbrV), nbrV);
        }
    }

    /**
     * 发送消息到邻居节点
     * <ol>
     *     <li> outerVertex,消息发送给邻居点所在 frag </li>
     *     <li> innerVertex,消息 local处理。Disk模式写入磁盘，Memory模式内存更新。下一超步处理</li>
     * </ol>
     *
     * @param frag           图数据
     * @param messageManager 消息管理器
     * @param msgVector      消息序列化OutputStream
     * @param msgToSend      消息列表
     * @param nbrV           邻居点
     * @param threadId       线程Id
     * @throws IOException
     */
    protected void sendMsgToOuterVertexOrUpdateInnerVertex(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager, FFIByteVectorOutputStream msgVector, List<LongArrayList> msgToSend, Vertex<Long> nbrV, int threadId) throws IOException {
        if (msgToSend.isEmpty()) {
            return;
        }

        if (frag.isOuterVertex(nbrV)) {
            msgVector.reset();

            // ------------------ write msg info start ------------------ //
            // 消息序列化
            msgVector.writeLong(frag.getOuterVertexGid(nbrV));
            msgVector.writeInt(msgToSend.size());
            for (LongArrayList path : msgToSend) {
                PathSerAndDeser.serialize(msgVector, path);
            }

            // 将序列化后的消息，远程发消息给邻居节点的 frag
            messageManager.sendToFragment(frag.getFragId(nbrV), msgVector.getVector(), threadId);
        } else {
            // Inner nbr 添加到 nextModified 集合，作为下一个迭代的起始节点
            ctx.nextModified.set(nbrV.getValue().intValue());

            sendToLocalFrag(frag, nbrV, msgToSend);
        }
    }

    /**
     * 接收消息
     * 并发执行
     *
     * @param graph          子图
     * @param messageManager 消息管理器
     */
    protected void receiveMessage(IFragment<Long, Long, Long, Long> graph, ParallelMessageManager messageManager) {
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
                            try {
                                receiveMessageImpl(graph, messageInBuffer);
                            } catch (Exception e) {
                                logger.error("Error when receiving message in fragment {} thread {}", graph.fid(), finalTid, e);
                            }
                        } else {
                            break;
                        }
                    }
                    if (ctx.diskStoreContext != null) {
                        // 将线程内 batch dump 剩余的 msg 刷盘
                        ctx.diskStoreContext.threadLocalMessageStorageFactory.dumpAll();
                        ctx.diskStoreContext.threadLocalMessageStorageFactory.remove();
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

        logger.info("received message end.Frag id is {},superStep is {},currModified vertex cnt is {},times is {}s", graph.fid(), ctx.superStep, ctx.currModified.cardinality(), (System.currentTimeMillis() - start) / 1000);
    }

    /**
     * 接收消息线程内实现
     *
     * @param graph  子图
     * @param buffer 消息缓冲区
     * @throws IOException
     */
    protected void receiveMessageImpl(IFragment<Long, Long, Long, Long> graph, MessageInBuffer buffer) throws IOException {
        FFIByteVector tmpVector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        Vertex<Long> tmpVertex = FFITypeFactoryhelper.newVertexLong();

        // 接收 发给本 frag 的所有消息
        List<LongArrayList> receivedMsg = new ArrayList<>(CircleUtil.DEFAULT_INITIAL_LIST_SIZE);
        while (buffer.getPureMessage(tmpVector)) {
            // The retrieved tmp vector has been resized, so the cached objAddress is not available.
            // trigger the refresh
            tmpVector.touch();

            FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(tmpVector);
            // tgt oid in msg
            long gid = inputStream.readLong();
            if (!graph.innerVertexGid2Vertex(gid, tmpVertex)) {
                logger.error("Fail to get lid from gid {}", gid);
            }

            ctx.currModified.set(tmpVertex.getValue().intValue());

            // ------------------ read msg info start ------------------ //
            int size = inputStream.readInt();

            if (size != 0) {
                for (int i = 0; i < size; i++) {
                    LongArrayList path = PathSerAndDeser.deserialize(inputStream);
                    // vertex path and edge path in msg must not be null
                    receivedMsg.add(path);
                }
            }
            // ------------------ read msg info end ------------------ //
            long oid = graph.getId(tmpVertex);
            processUponReceiveMsg(graph, tmpVertex.getValue().intValue(), oid, receivedMsg);

            if (showLog(graph)) {
                logger.info("vprog  end.superStep is {},lid is {},oid is {},vdata is {},msg is {}", ctx.superStep, tmpVertex.getValue().intValue(), oid, getVertexPath(tmpVertex.getValue().intValue(), null), receivedMsg);
            }
            tmpVector.clear();
            receivedMsg.clear();
        }

        tmpVector.delete();
    }

    /**
     * 多线程发送消息，可以区分批次
     *
     * @param frag           子图
     * @param messageManager 消息管理器
     * @param begin          单个批次内的起始节点
     * @param end            单个批次内的结束节点
     * @param batchId        批次id
     */
    public void sendMessageThroughOE(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager, long begin, long end, int batchId) {
        CountDownLatch countDownLatch = new CountDownLatch(ctx.threadNum);
        Map<Integer, long[]> rangeMap = splitBeginAndEnd(begin, end, ctx.threadNum);

        for (int tid = 0; tid < ctx.threadNum; ++tid) {
            long threadBegin = rangeMap.get(tid)[0];
            long threadEnd = rangeMap.get(tid)[1];
            int finalTid = tid;
            ctx.executor.execute(() -> {
                try {
                    sendMessageThroughOEImpl(frag, threadBegin, threadEnd, messageManager, finalTid, batchId);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                ctx.removeTheadLocalVariable();
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            logger.error("sendMessageThroughOE error.", e);
            ctx.executor.shutdown();
        }
    }

    /**
     * 单个线程发送消息实现
     *
     * @param frag           子图
     * @param startVertex    起始点
     * @param endVertex      结束点
     * @param messageManager 消息管理器
     * @param threadId       线程id
     * @param batchId        批次id
     * @throws IOException
     */
    private void sendMessageThroughOEImpl(IFragment<Long, Long, Long, Long> frag, long startVertex, long endVertex, ParallelMessageManager messageManager, int threadId, int batchId) throws IOException {
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (long i = startVertex; i < endVertex; ++i) {
            if (ctx.currModified.get((int) i)) {
                vertex.setValue(i);
                sendToNbr(frag, messageManager, vertex, threadId, batchId);
            }
        }
    }

    /**
     * 发送消息给邻居节点。区分有向/无向
     *
     * @param frag           子图
     * @param messageManager 消息管理器
     * @param vertex         当前点
     * @param threadId       线程 id，0~threadNum-1
     *                       发送给 frag 时需要指定 threadId
     * @param batchId        超步内批次 id，0~fragBatchNum-1
     *                       <ol>
     *                          <li>memory 模式:batchId 固定为0</li>
     *                          <li>disk 模式: 超步内可以分批次执行，减轻内存压力</li>
     *                       </ol>
     * @throws IOException
     */
    public void sendToNbr(IFragment<Long, Long, Long, Long> frag, ParallelMessageManager messageManager, Vertex<Long> vertex, int threadId, int batchId) throws IOException {
        // src -> dst
        sendToAdjList(frag, messageManager, vertex, true, threadId, batchId);
        if (!ctx.isDirected) {
            // dst -> src
            sendToAdjList(frag, messageManager, vertex, false, threadId, batchId);
        }
        // 清除 点的 current 属性
        ctx.clearVertexInMemory(vertex);
    }

    /**
     * 将较大范围的 long 区间按批次拆分成小范围，生成不同的 start 和 end
     *
     * @param begin    起始点id
     * @param end      结束点id
     * @param batchNum 批次数
     * @return 线程id -> [begin, end]
     */
    public Map<Integer, long[]> splitBeginAndEnd(long begin, long end, int batchNum) {
        long chunkSize = (end - begin) / batchNum;
        return IntStream.range(0, batchNum).boxed().collect(Collectors.toMap(i -> i, i -> {
            long batchBegin = begin + i * chunkSize;
            long batchEnd = begin + (i + 1) * chunkSize;
            if (i == batchNum - 1) {
                batchEnd = end;
            }
            return new long[]{batchBegin, batchEnd};
        }));
    }

    protected boolean showLog(IFragment<Long, Long, Long, Long> frag) {
        return frag.getInnerVerticesNum() < 30;
    }
}
