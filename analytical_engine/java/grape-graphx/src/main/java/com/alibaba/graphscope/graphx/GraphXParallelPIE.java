/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;
import com.alibaba.graphscope.ds.BaseTypedArray;
import com.alibaba.graphscope.ds.PrimitiveTypedArray;
import com.alibaba.graphscope.ds.StringTypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.BaseArrowProjectedFragment;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.graphx.graph.GSEdgeTripletImpl;
import com.alibaba.graphscope.graphx.utils.DoubleDouble;
import com.alibaba.graphscope.graphx.utils.IdParser;
import com.alibaba.graphscope.parallel.MessageInBuffer;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.serialization.FakeFFIByteVectorInputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.alibaba.graphscope.stdcxx.FakeFFIByteVector;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.InterruptibleTriConsumer;
import com.alibaba.graphscope.utils.LongPointerAccessor;
import com.alibaba.graphscope.utils.MessageStore;
import com.alibaba.graphscope.utils.ThreadSafeBitSet;
import com.alibaba.graphscope.utils.array.PrimitiveArray;

import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeTriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class GraphXParallelPIE<VD, ED, MSG_T> {

    private static Logger logger = LoggerFactory.getLogger(GraphXParallelPIE.class.getName());
    private static int BATCH_SIZE = 8192;

    /**
     * User vertex program: vprog: (VertexId, VD, A) => VD
     */
    private Function3<Long, VD, MSG_T, VD> vprog;
    /**
     * EdgeTriplet[VD, ED] => Iterator[(VertexId, A)]
     */
    private Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> sendMsg;
    /**
     * (A, A) => A)
     */
    protected Function2<MSG_T, MSG_T, MSG_T> mergeMsg;

    protected IFragment<Long, Long, VD, ED> iFragment; // different from c++ frag
    protected BaseArrowProjectedFragment<Long, Long, VD, ED> projectedFragment;
    private MSG_T initialMessage;
    private ExecutorService executorService;
    private int numCores, maxIterations, round;
    private long vprogTime, msgSendTime, receiveTime, flushTime, bitsetTime;
    private GraphXConf<VD, ED, MSG_T> conf;
    //    private GSEdgeTripletImpl<VD, ED> edgeTriplet;
    ParallelMessageManager messageManager;
    private PrimitiveArray<VD> newVdataArray;
    private PrimitiveArray<ED> newEdataArray;
    private int fid;
    private IdParser idParser;
    /**
     * The messageStore stores the result messages after query. 1) Before PEval or IncEval,
     * messageStore should be clear. 2) After iterateOnEdges, we need to flush messages a. For
     * message to inner vertex, set them locally b. For message to outer vertex, send via mpi 3)
     * after flush, clear message store.
     */
    private MessageStore<MSG_T> messageStore;

    private int innerVerticesNum, verticesNum;
    private ThreadSafeBitSet curSet, nextSet;
    private EdgeDirection direction;
    private long[] lid2Oid;
    private PrimitiveTypedArray<Long> outerLid2Gid;
    private long oeBeginAddress, ieBeginAddress;
    //    private TypedArray<Long> oeOffsetArray, ieOffsetArray;
    private LongPointerAccessor oeOffsetBeginArray,
            oeOffsetEndArray,
            ieOffsetBeginArray,
            ieOffsetEndArray;
    private int[] fid2WorkerId;
    private MessageInBuffer.Factory bufferFactory = FFITypeFactoryhelper.newMessageInBuffer();

    public PrimitiveArray<VD> getNewVdataArray() {
        return newVdataArray;
    }

    public BaseArrowProjectedFragment<Long, Long, VD, ED> getProjectedFragment() {
        return projectedFragment;
    }

    public GraphXParallelPIE(
            GraphXConf<VD, ED, MSG_T> conf,
            Function3<Long, VD, MSG_T, VD> vprog,
            Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> sendMsg,
            Function2<MSG_T, MSG_T, MSG_T> mergeMsg,
            MSG_T initialMessage,
            EdgeDirection direction) {
        this.conf = conf;
        this.vprog = vprog;
        this.sendMsg = sendMsg;
        this.mergeMsg = mergeMsg;
        this.initialMessage = initialMessage;
        this.direction = direction;
    }

    public void init(
            IFragment<Long, Long, VD, ED> fragment,
            ParallelMessageManager messageManager,
            int maxIterations,
            int parallelism,
            String workerIdToFid)
            throws IOException, ClassNotFoundException {
        long time0 = System.nanoTime();
        this.iFragment = fragment;
        fid = fragment.fid();
        idParser = new IdParser(fragment.fnum());
        this.numCores = parallelism;
        if (!iFragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
            throw new IllegalStateException("Only support projected fragment");
        }
        this.projectedFragment = getProjectedFragment(iFragment);
        innerVerticesNum = (int) projectedFragment.getInnerVerticesNum();
        verticesNum = projectedFragment.getVerticesNum().intValue();
        long time00 = System.nanoTime();
        Tuple2<PrimitiveArray<VD>, PrimitiveArray<ED>> tuple =
                initOldAndNewArrays(projectedFragment, conf);
        long time01 = System.nanoTime();
        newVdataArray = tuple._1();
        newEdataArray = tuple._2();
        curSet =
                new ThreadSafeBitSet(
                        ThreadSafeBitSet.DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS, verticesNum);
        curSet.setUntil(verticesNum);
        logger.info(" after init curSet, size {}", curSet.cardinality());
        // initially activate all vertices

        this.messageManager = messageManager;
        this.messageManager.initChannels(numCores);
        this.maxIterations = maxIterations;

        nextSet =
                new ThreadSafeBitSet(
                        ThreadSafeBitSet.DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS, verticesNum);
        this.messageStore =
                MessageStore.create(
                        (int) verticesNum,
                        fragment.fnum(),
                        numCores,
                        conf.getMsgClass(),
                        mergeMsg,
                        nextSet,
                        conf,
                        innerVerticesNum);
        logger.debug("ivnum {}, tvnum {}", innerVerticesNum, verticesNum);

        round = 0;
        lid2Oid = new long[projectedFragment.getVerticesNum().intValue()];
        //        GraphXVertexMap<Long, Long> vm = projectedFragment.getVM();
        {
            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
            for (int i = 0; i < lid2Oid.length; ++i) {
                vertex.setValue((long) i);
                lid2Oid[i] = projectedFragment.getId(vertex);
            }
        }

        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        vertex.setValue(0L);
        //        oeOffsetArray = projectedFragment.getCSR().getOEOffsetsArray();
        oeOffsetBeginArray = new LongPointerAccessor(projectedFragment.getOEOffsetsBeginPtr());
        oeOffsetEndArray = new LongPointerAccessor(projectedFragment.getOEOffsetsEndPtr());
        ieOffsetBeginArray = new LongPointerAccessor(projectedFragment.getIEOffsetsBeginPtr());
        ieOffsetEndArray = new LongPointerAccessor(projectedFragment.getIEOffsetsEndPtr());

        oeBeginAddress = projectedFragment.getOutEdgesPtr().getAddress();
        ieBeginAddress = projectedFragment.getInEdgesPtr().getAddress();

        executorService = Executors.newFixedThreadPool(numCores);
        logger.info("Parallelism for frag {} is {}", projectedFragment.fid(), numCores);
        fid2WorkerId = new int[projectedFragment.fnum()];
        fillFid2WorkerId(workerIdToFid);
        msgSendTime = vprogTime = receiveTime = flushTime = bitsetTime = 0;
        long time1 = System.nanoTime();
        logger.info(
                "[Perf:] init cost {}ms, copy array cost {}ms",
                (time1 - time0) / 1000000,
                (time01 - time00) / 1000000);
    }

    long getId(int lid) {
        return lid2Oid[lid];
    }

    public int curRound() {
        return round;
    }

    private void runVProg(int startLid, int endLid, boolean firstRound) {
        for (int lid = curSet.nextSetBit(startLid);
                lid >= 0 && lid < endLid;
                lid = curSet.nextSetBit(lid + 1)) {
            long oid = lid2Oid[lid];
            VD originalVD = newVdataArray.get(lid);
            // if (originalVD == null){
            // null indicate the vertex is inactive.
            //  continue ;
            // }
            if (firstRound) {
                if (oid == 1) {
                    logger.info(
                            "frag {} oid 1 lid {},ivnum {} vd {}",
                            iFragment.fid(),
                            lid,
                            innerVerticesNum,
                            originalVD);
                }
                newVdataArray.set(lid, vprog.apply(oid, originalVD, initialMessage));
            } else {
                newVdataArray.set(lid, vprog.apply(oid, originalVD, messageStore.get(lid)));
            }
        }
    }

    private void iterateEdge(int startLid, int endLid, int threadId) throws InterruptedException {
        GSEdgeTripletImpl<VD, ED> edgeTriplet = new GSEdgeTripletImpl<>();
        long beginOffset, endOffset;
        for (int lid = curSet.nextSetBit(startLid);
                lid >= 0 && lid < endLid;
                lid = curSet.nextSetBit(lid + 1)) {
            if (newVdataArray.get(lid) == null) {
                throw new IllegalStateException("received null vertex data");
            }
            edgeTriplet.setSrcOid(lid2Oid[lid], newVdataArray.get(lid));

            beginOffset = oeOffsetBeginArray.get(lid);
            endOffset = oeOffsetEndArray.get(lid);
            long address = oeBeginAddress + (beginOffset << 4);
            while (beginOffset < endOffset) {
                int nbrVid = (int) JavaRuntime.getLong(address);
                int eid = (int) JavaRuntime.getLong(address + 8);
                VD dstAttr = newVdataArray.get(nbrVid);
                if (dstAttr != null) {
                    edgeTriplet.setDstOid(getId(nbrVid), dstAttr);
                    edgeTriplet.setAttr(newEdataArray.get(eid));
                    Iterator<Tuple2<Long, MSG_T>> msgs = sendMsg.apply(edgeTriplet);
                    //                    logger.info("visiting edge {}->{}, {}, result msg {}",
                    // edgeTriplet.srcId(), edgeTriplet.dstId(), edgeTriplet.attr(),
                    // msgs.isEmpty());
                    if (msgs.nonEmpty()) {
                        //                        logger.info("msg to send when visiting edge
                        // {}->{}, {}"
                        //                            ,edgeTriplet.srcId(), edgeTriplet.dstId(),
                        // edgeTriplet.attr());
                        //                        if (nbrVid >= innerVerticesNum){
                        //                            logger.info("inner vertex {} has msg to or
                        // from dst {}", lid, nbrVid);
                        //                        }
                        messageStore.addMessages(
                                msgs, threadId, edgeTriplet, iFragment, lid, nbrVid);
                    }
                }
                address += 16;
                beginOffset += 1;
            }
        }
    }

    public void parallelExecute(
            InterruptibleTriConsumer<Integer, Integer, Integer> function, int limit) {
        AtomicInteger getter = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(numCores);
        for (int tid = 0; tid < numCores; ++tid) {
            final int finalTid = tid;
            executorService.execute(
                    () -> {
                        int begin, end;
                        while (true) {
                            begin = Math.min(getter.getAndAdd(BATCH_SIZE), limit);
                            end = Math.min(begin + BATCH_SIZE, limit);
                            if (begin >= end) {
                                break;
                            }
                            try {
                                function.accept(begin, end, finalTid);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        countDownLatch.countDown();
                    });
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
            executorService.shutdown();
        }
    }

    public void syncOuterVertexData() {
        parallelExecute(
                new InterruptibleTriConsumer<Integer, Integer, Integer>() {
                    @Override
                    public void accept(Integer startLid, Integer endLid, Integer threadId)
                            throws InterruptedException {
                        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                        for (int i = startLid; i < endLid; ++i) {
                            vertex.setValue((long) i);
                            messageStore.sendMsgThroughIEdges(
                                    vertex,
                                    newVdataArray.get(i),
                                    threadId,
                                    messageManager,
                                    iFragment);
                        }
                    }
                },
                innerVerticesNum);
        // no need to flush
        round = 1;
        logger.info("Frag {} finish send message to outer vertices", iFragment.fid());
    }

    public void parallelPEval() {
        if (fid == 0) {
            logger.info("[Start PEval]");
        }
        // Received the synced outer vertices in previous round.
        receiveTime -= System.nanoTime();
        ///////////////////////////////////// Receive message////////////////////
        receiveEdgeMessage();
        // This will set received outer vertices to active, but shouldn't affect us.
        receiveTime += System.nanoTime();

        vprogTime -= System.nanoTime();
        // We need to update outer vertex message to vd array, otherwise, we will send out message
        // infinitely.
        parallelExecute((begin, end, threadId) -> runVProg(begin, end, true), verticesNum);
        vprogTime += System.nanoTime();

        msgSendTime -= System.nanoTime();
        parallelExecute(this::iterateEdge, innerVerticesNum);
        msgSendTime += System.nanoTime();
        logger.info("[PEval] Finish iterate edges for frag {}", projectedFragment.fid());
        flushTime -= System.nanoTime();
        try {
            messageStore.flushMessages(
                    nextSet, messageManager, iFragment, fid2WorkerId, executorService);
        } catch (IOException e) {
            e.printStackTrace();
        }
        flushTime += System.nanoTime();
        round = 2;
    }

    public boolean parallelIncEval() {
        if (round >= maxIterations) {
            return true;
        }

        bitsetTime -= System.nanoTime();
        curSet.clearAll();
        logger.debug(
                "before union curset {} nextset {}", curSet.cardinality(), nextSet.cardinality());
        curSet = ThreadSafeBitSet.orAll(curSet, nextSet);
        logger.debug(
                "after union curset {} nextset {}", curSet.cardinality(), nextSet.cardinality());
        nextSet.clearAll();
        bitsetTime += System.nanoTime();

        receiveTime -= System.nanoTime();
        ///////////////////////////////////// Receive message////////////////////
        receiveMessage();
        receiveTime += System.nanoTime();

        if (curSet.cardinality() > 0) {
            logger.debug(
                    "Before running round {}, frag [{}] has {} active vertices",
                    round,
                    projectedFragment.fid(),
                    curSet.cardinality());
            vprogTime -= System.nanoTime();
            parallelExecute((begin, end, threadId) -> runVProg(begin, end, false), verticesNum);
            vprogTime += System.nanoTime();

            msgSendTime -= System.nanoTime();
            parallelExecute(this::iterateEdge, innerVerticesNum);
            msgSendTime += System.nanoTime();
            logger.debug(
                    "[IncEval {}] Finish iterate edges for frag {}",
                    round,
                    projectedFragment.fid());
            flushTime -= System.nanoTime();
            try {
                messageStore.flushMessages(
                        nextSet, messageManager, iFragment, fid2WorkerId, executorService);
            } catch (IOException e) {
                e.printStackTrace();
            }
            flushTime += System.nanoTime();
        } else {
            logger.info("Frag {} No message received", projectedFragment.fid());
            round += 1;
            return true;
        }
        round += 1;
        logger.info(
                "Round [{}] vprog {}, msgSend {} flushMsg {}, receive time {}, bitset time {}",
                round,
                vprogTime / 1000000,
                msgSendTime / 1000000,
                flushTime / 1000000,
                receiveTime / 1000000,
                bitsetTime / 1000000);
        return false;
    }

    /**
     * To receive message from grape, we need some wrappers. double -> DoubleMessage. long ->
     * LongMessage. Vprog happens here
     *
     * @return true if message received.
     */
    private void receiveMessage() {
        CountDownLatch countDownLatch = new CountDownLatch(numCores);
        for (int tid = 0; tid < numCores; ++tid) {
            final int finalTid = tid;
            executorService.execute(
                    () -> {
                        MessageInBuffer messageInBuffer = bufferFactory.create();
                        FFIByteVector tmpVector =
                                (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
                        long bytesOfReceivedMsg = 0;
                        while (messageManager.getMessageInBuffer(messageInBuffer)) {
                            logger.info("thread {} got message inbuffer", finalTid);
                            messageInBuffer.getPureMessage(tmpVector);
                            tmpVector.touch();
                            logger.debug(
                                    "Frag [{}] digest message of size {}",
                                    projectedFragment.fid(),
                                    tmpVector.size());

                            messageStore.digest(iFragment, tmpVector, curSet, finalTid);
                            bytesOfReceivedMsg += tmpVector.size();
                        }
                        logger.debug(
                                "Frag [{}] Totally received {} bytes",
                                projectedFragment.fid(),
                                bytesOfReceivedMsg);
                        countDownLatch.countDown();
                    });
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
            executorService.shutdown();
        }
    }

    /**
     * Receive message sent via sendMsgThoughIEdges.
     */
    private void receiveEdgeMessage() {
        CountDownLatch countDownLatch = new CountDownLatch(numCores);
        for (int tid = 0; tid < numCores; ++tid) {
            final int finalTid = tid;
            executorService.execute(
                    () -> {
                        MessageInBuffer messageInBuffer = bufferFactory.create();
                        long receivedMsgSize = 0;
                        while (messageManager.getMessageInBuffer(messageInBuffer)) {
                            long msgReceived =
                                    messageStore.digest(
                                            iFragment, messageInBuffer, curSet, finalTid);
                            receivedMsgSize += msgReceived;
                        }
                        logger.debug(
                                "Frag [{}] Totally received {} msg",
                                projectedFragment.fid(),
                                receivedMsgSize);
                        countDownLatch.countDown();
                    });
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
            executorService.shutdown();
        }
    }

    private static <VD_T, ED_T, MSG_T_>
            Tuple2<PrimitiveArray<VD_T>, PrimitiveArray<ED_T>> initOldAndNewArrays(
                    BaseArrowProjectedFragment<Long, Long, VD_T, ED_T> projectedFragment,
                    GraphXConf<VD_T, ED_T, MSG_T_> conf)
                    throws IOException, ClassNotFoundException {

        // For vd array
        BaseTypedArray<VD_T> oldVdataArray =
                ((ArrowProjectedFragment<Long, Long, VD_T, ED_T>) projectedFragment)
                        .getVdataArrayAccessor();

        PrimitiveArray<VD_T> newVdataArray;
        {
            if (conf.isVDPrimitive()) {
                if (oldVdataArray.getLength() != projectedFragment.getInnerVerticesNum()) {
                    throw new IllegalStateException(
                            "not equal"
                                    + oldVdataArray.getLength()
                                    + ","
                                    + projectedFragment.getInnerVerticesNum());
                }

                PrimitiveTypedArray<VD_T> tmp =
                        FFITypeFactoryhelper.newPrimitiveTypedArray(conf.getVdClass());
                tmp.setAddress(oldVdataArray.getAddress());
                newVdataArray =
                        processPrimitiveArray(
                                tmp,
                                conf.getVdClass(),
                                projectedFragment.getVerticesNum().intValue());
            } else {
                StringTypedArray tmp = FFITypeFactoryhelper.newStringTypedArray();
                tmp.setAddress(oldVdataArray.getAddress());
                // construct typed array to make use.
                newVdataArray =
                        processComplexArray(
                                tmp,
                                conf.getVdClass(),
                                projectedFragment.getVerticesNum().intValue());
            }
        }
        BaseTypedArray<ED_T> oldEdataArray =
                ((ArrowProjectedFragment<Long, Long, VD_T, ED_T>) projectedFragment)
                        .getEdataArrayAccessor();
        PrimitiveArray<ED_T> newEdataArray;
        {
            if (conf.isEDPrimitive()) {
                PrimitiveTypedArray<ED_T> tmp =
                        FFITypeFactoryhelper.newPrimitiveTypedArray(conf.getEdClass());
                tmp.setAddress(oldEdataArray.getAddress());
                newEdataArray = wrapReadOnlyArray(tmp, conf.getEdClass());
            } else {
                StringTypedArray tmp = FFITypeFactoryhelper.newStringTypedArray();
                tmp.setAddress(oldEdataArray.getAddress());
                // construct typed array to make use.
                newEdataArray =
                        processComplexArray(
                                tmp, conf.getEdClass(), (int) oldEdataArray.getLength());
            }
        }
        return new Tuple2<>(newVdataArray, newEdataArray);
    }

    public static <T> PrimitiveArray<T> processComplexArray(
            StringTypedArray oldArray, Class<? extends T> clz, int length)
            throws IOException, ClassNotFoundException {
        if (length < oldArray.getLength()) {
            throw new IllegalStateException(
                    "dst array length can not be smaller than off heap array "
                            + length
                            + ", "
                            + oldArray.getLength());
        }
        FakeFFIByteVector vector =
                new FakeFFIByteVector(oldArray.getRawData(), oldArray.getRawDataLength());
        FakeFFIByteVectorInputStream ffiInput = new FakeFFIByteVectorInputStream(vector);
        long len = oldArray.getLength();
        logger.info("reading {} objects from array of bytes {}", len, oldArray.getLength());
        PrimitiveArray<T> newArray = PrimitiveArray.create(clz, length);
        if (clz.equals(DoubleDouble.class)) {
            for (int i = 0; i < len; ++i) {
                double a = ffiInput.readDouble();
                double b = ffiInput.readDouble();
                newArray.set(i, (T) new DoubleDouble(a, b));
            }
        } else {
            ObjectInputStream objectInputStream = new ObjectInputStream(ffiInput);
            for (int i = 0; i < len; ++i) {
                T obj = (T) objectInputStream.readObject();
                newArray.set(i, obj);
            }
        }
        return newArray;
    }

    private static <T> PrimitiveArray<T> wrapReadOnlyArray(
            PrimitiveTypedArray<T> oldArray, Class<? extends T> clz) {
        return PrimitiveArray.createImmutable(oldArray, clz);
    }

    /**
     * For vertex array, we need to reserve outer vertices size
     */
    private static <T> PrimitiveArray<T> processPrimitiveArray(
            PrimitiveTypedArray<T> oldArray, Class<? extends T> clz, int length) {
        if (length < oldArray.getLength()) {
            throw new IllegalStateException(
                    "dst array length can not be smaller than off heap array "
                            + length
                            + ", "
                            + oldArray.getLength());
        }
        PrimitiveArray<T> newArray = PrimitiveArray.create(clz, length);
        long len = oldArray.getLength();
        for (int i = 0; i < len; ++i) {
            newArray.set(i, oldArray.get(i));
        }
        return newArray;
    }

    private BaseArrowProjectedFragment<Long, Long, VD, ED> getProjectedFragment(
            IFragment<Long, Long, VD, ED> iFragment) {
        if (iFragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
            ArrowProjectedFragment<Long, Long, VD, ED> res =
                    ((ArrowProjectedAdaptor<Long, Long, VD, ED>) iFragment)
                            .getArrowProjectedFragment();
            return res;
        } else {
            throw new IllegalStateException("Not implemented" + iFragment);
        }
    }

    private void fillFid2WorkerId(String str) {
        String[] splited = str.split(";");
        if (splited.length != fid2WorkerId.length) {
            throw new IllegalStateException(
                    "length neq " + splited.length + "," + fid2WorkerId.length);
        }
        for (String tuple : splited) {
            String[] tmp = tuple.split(":");
            if (tmp.length != 2) {
                throw new IllegalStateException("length neq 2" + tmp.length);
            }
            int workerId = Integer.parseInt(tmp[0]);
            int fid = Integer.parseInt(tmp[1]);
            fid2WorkerId[fid] = workerId;
        }
    }
}
