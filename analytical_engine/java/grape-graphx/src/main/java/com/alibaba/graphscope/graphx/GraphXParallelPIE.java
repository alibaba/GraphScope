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
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.StringTypedArray;
import com.alibaba.graphscope.ds.TypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.fragment.GraphXFragment;
import com.alibaba.graphscope.fragment.GraphXStringEDFragment;
import com.alibaba.graphscope.fragment.GraphXStringVDFragment;
import com.alibaba.graphscope.fragment.GraphXStringVEDFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.GraphXFragmentAdaptor;
import com.alibaba.graphscope.fragment.adaptor.GraphXStringEDFragmentAdaptor;
import com.alibaba.graphscope.fragment.adaptor.GraphXStringVDFragmentAdaptor;
import com.alibaba.graphscope.fragment.adaptor.GraphXStringVEDFragmentAdaptor;
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
    protected BaseGraphXFragment<Long, Long, VD, ED> graphXFragment;
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
    private TypedArray<Long>[] lid2Oid;
    private TypedArray<Long> outerLid2Gid;
    private PropertyNbrUnit<Long>[] nbrs;
    private long oeBeginAddress, ieBeginAddress;
    private TypedArray<Long> oeOffsetArray, ieOffsetArray;
    private int[] fid2WorkerId;
    private MessageInBuffer.Factory bufferFactory = FFITypeFactoryhelper.newMessageInBuffer();

    public PrimitiveArray<VD> getNewVdataArray() {
        return newVdataArray;
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
        if (!(iFragment.fragmentType().equals(FragmentType.GraphXFragment)
                || iFragment.fragmentType().equals(FragmentType.GraphXStringVDFragment)
                || iFragment.fragmentType().equals(FragmentType.GraphXStringEDFragment)
                || iFragment.fragmentType().equals(FragmentType.GraphXStringVEDFragment))) {
            throw new IllegalStateException("Only support graphx fragment");
        }
        this.graphXFragment = getBaseGraphXFragment(iFragment);
        innerVerticesNum = (int) graphXFragment.getInnerVerticesNum();
        verticesNum = graphXFragment.getVerticesNum().intValue();
        long time00 = System.nanoTime();
        Tuple2<PrimitiveArray<VD>, PrimitiveArray<ED>> tuple =
                initOldAndNewArrays(graphXFragment, conf);
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
        logger.info("before create store");
        this.messageStore =
                MessageStore.create(
                        (int) verticesNum,
                        fragment.fnum(),
                        numCores,
                        conf.getMsgClass(),
                        mergeMsg,
                        nextSet);
        logger.info("after create store");
        logger.debug("ivnum {}, tvnum {}", innerVerticesNum, verticesNum);

        round = 0;
        lid2Oid = new TypedArray[graphXFragment.fnum()];
        GraphXVertexMap<Long, Long> vm = graphXFragment.getVM();
        for (int i = 0; i < vm.fnum(); ++i) {
            lid2Oid[i] = vm.getLid2OidAccessor(i);
        }
        outerLid2Gid = vm.getOuterLid2GidAccessor();

        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        vertex.SetValue(0L);
        oeOffsetArray = graphXFragment.getCSR().getOEOffsetsArray();
        ieOffsetArray = graphXFragment.getCSR().getIEOffsetsArray();
        nbrs = new PropertyNbrUnit[parallelism];
        for (int i = 0; i < parallelism; ++i) {
            nbrs[i] = graphXFragment.getOEBegin(vertex);
        }

        oeBeginAddress = graphXFragment.getOEBegin(vertex).getAddress();
        ieBeginAddress = graphXFragment.getIEBegin(vertex).getAddress();

        executorService = Executors.newFixedThreadPool(numCores);
        logger.info("Parallelism for frag {} is {}", graphXFragment.fid(), numCores);
        fid2WorkerId = new int[graphXFragment.fnum()];
        fillFid2WorkerId(workerIdToFid);
        msgSendTime = vprogTime = receiveTime = flushTime = bitsetTime = 0;
        long time1 = System.nanoTime();
        logger.info(
                "[Perf:] init cost {}ms, copy array cost {}ms",
                (time1 - time0) / 1000000,
                (time01 - time00) / 1000000);
    }

    long getId(int lid) {
        if (lid < innerVerticesNum) {
            return lid2Oid[fid].get(lid);
        } else {
            long gid = outerLid2Gid.get(lid - innerVerticesNum);
            long outerLid = idParser.getLocalId(gid);
            int fid = idParser.getFragId(gid);
            return lid2Oid[fid].get(outerLid);
        }
    }

    private void runVProg(int startLid, int endLid, boolean firstRound) {
        for (int lid = curSet.nextSetBit(startLid);
                lid >= 0 && lid < endLid;
                lid = curSet.nextSetBit(lid + 1)) {
            long oid = lid2Oid[fid].get(lid);
            VD originalVD = newVdataArray.get(lid);
            // if (originalVD == null){
            // null indicate the vertex is inactive.
            //  continue ;
            // }
            if (firstRound) {
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
            edgeTriplet.setSrcOid(lid2Oid[fid].get(lid), newVdataArray.get(lid));

            beginOffset = oeOffsetArray.get(lid);
            endOffset = oeOffsetArray.get(lid + 1);
            long address = oeBeginAddress + (beginOffset << 4);
            while (beginOffset < endOffset) {
                int nbrVid = (int) JavaRuntime.getLong(address);
                int eid = (int) JavaRuntime.getLong(address + 8);
                VD dstAttr = newVdataArray.get(nbrVid);
                if (dstAttr != null) {
                    edgeTriplet.setDstOid(getId(nbrVid), dstAttr);
                    edgeTriplet.setAttr(newEdataArray.get(eid));
                    Iterator<Tuple2<Long, MSG_T>> msgs = sendMsg.apply(edgeTriplet);
                    if (!msgs.equals(Iterator.empty())) {
                        messageStore.addMessages(
                                msgs, graphXFragment, threadId, edgeTriplet, lid, nbrVid);
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

    public void ParallelPEval() {
        if (fid == 0) {
            logger.info("[Start PEval]");
        }
        vprogTime -= System.nanoTime();
        // We need to update outer vertex message to vd array, otherwise, we will send out message
        // infinitely.
        parallelExecute((begin, end, threadId) -> runVProg(begin, end, true), verticesNum);
        vprogTime += System.nanoTime();

        msgSendTime -= System.nanoTime();
        parallelExecute(this::iterateEdge, innerVerticesNum);
        msgSendTime += System.nanoTime();
        logger.debug("[PEval] Finish iterate edges for frag {}", graphXFragment.fid());
        flushTime -= System.nanoTime();
        try {
            messageStore.flushMessages(
                    nextSet, messageManager, graphXFragment, fid2WorkerId, executorService);
        } catch (IOException e) {
            e.printStackTrace();
        }
        flushTime += System.nanoTime();
        round = 1;
    }

    public boolean ParallelIncEval() {
        if (round >= maxIterations) {
            return true;
        }

        bitsetTime -= System.nanoTime();
        curSet.clearAll();
        logger.info(
                "before union curset {} nextset {}", curSet.cardinality(), nextSet.cardinality());
        curSet = ThreadSafeBitSet.orAll(curSet, nextSet);
        logger.info(
                "after union curset {} nextset {}", curSet.cardinality(), nextSet.cardinality());
        nextSet.clearAll();
        bitsetTime += System.nanoTime();

        receiveTime -= System.nanoTime();
        ///////////////////////////////////// Receive message////////////////////
        receiveMessage();
        receiveTime += System.nanoTime();

        if (curSet.cardinality() > 0) {
            logger.info(
                    "Before running round {}, frag [{}] has {} active vertices",
                    round,
                    graphXFragment.fid(),
                    curSet.cardinality());
            vprogTime -= System.nanoTime();
            parallelExecute((begin, end, threadId) -> runVProg(begin, end, false), verticesNum);
            vprogTime += System.nanoTime();

            msgSendTime -= System.nanoTime();
            parallelExecute(this::iterateEdge, innerVerticesNum);
            msgSendTime += System.nanoTime();
            logger.debug(
                    "[IncEval {}] Finish iterate edges for frag {}", round, graphXFragment.fid());
            flushTime -= System.nanoTime();
            try {
                messageStore.flushMessages(
                        nextSet, messageManager, graphXFragment, fid2WorkerId, executorService);
            } catch (IOException e) {
                e.printStackTrace();
            }
            flushTime += System.nanoTime();
        } else {
            logger.info("Frag {} No message received", graphXFragment.fid());
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
                            logger.info(
                                    "Frag [{}] digest message of size {}",
                                    graphXFragment.fid(),
                                    tmpVector.size());

                            messageStore.digest(tmpVector, graphXFragment, curSet);
                            bytesOfReceivedMsg += tmpVector.size();
                        }
                        logger.debug(
                                "Frag [{}] Totally received {} bytes",
                                graphXFragment.fid(),
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

    private static <VD_T, ED_T, MSG_T_>
            Tuple2<PrimitiveArray<VD_T>, PrimitiveArray<ED_T>> initOldAndNewArrays(
                    BaseGraphXFragment<Long, Long, VD_T, ED_T> baseGraphXFragment,
                    GraphXConf<VD_T, ED_T, MSG_T_> conf)
                    throws IOException, ClassNotFoundException {
        // For vd array
        if (conf.isVDPrimitive() && conf.isEDPrimitive()) {
            TypedArray<VD_T> oldVdataArray;
            TypedArray<ED_T> oldEdataArray;
            GraphXFragment<Long, Long, VD_T, ED_T> graphXFragment =
                    (GraphXFragment<Long, Long, VD_T, ED_T>) baseGraphXFragment;
            oldEdataArray = graphXFragment.getEdataArray();
            oldVdataArray = graphXFragment.getVdataArray();
            logger.info(
                    "vdata array size {}, frag vnum{}",
                    oldVdataArray.getLength(),
                    graphXFragment.getVerticesNum());
            if (oldVdataArray.getLength() != graphXFragment.getVerticesNum()) {
                throw new IllegalStateException(
                        "not equal"
                                + oldVdataArray.getLength()
                                + ","
                                + graphXFragment.getVerticesNum());
            }
            PrimitiveArray<ED_T> newEdataArray =
                    wrapReadOnlyArray(oldEdataArray, conf.getEdClass());
            // Should contain outer vertices
            PrimitiveArray<VD_T> newVdataArray =
                    processPrimitiveArray(oldVdataArray, conf.getVdClass());
            return new Tuple2<>(newVdataArray, newEdataArray);
        } else if (!conf.isVDPrimitive() && conf.isEDPrimitive()) {
            StringTypedArray oldVdataArray;
            TypedArray<ED_T> oldEdataArray;
            GraphXStringVDFragment<Long, Long, VD_T, ED_T> graphXFragment =
                    (GraphXStringVDFragment<Long, Long, VD_T, ED_T>) baseGraphXFragment;
            oldEdataArray = graphXFragment.getEdataArray();
            oldVdataArray = graphXFragment.getVdataArray();
            logger.info(
                    "total bytes in vd array {}, vertices num {}",
                    oldVdataArray.getLength(),
                    graphXFragment.getVerticesNum());

            PrimitiveArray<ED_T> newEdataArray =
                    wrapReadOnlyArray(oldEdataArray, conf.getEdClass());
            PrimitiveArray<VD_T> newVdataArray =
                    processComplexArray(oldVdataArray, conf.getVdClass());
            return new Tuple2<>(newVdataArray, newEdataArray);
        } else if (conf.isVDPrimitive() && !conf.isEDPrimitive()) {
            StringTypedArray oldEDdataArray;
            TypedArray<VD_T> oldVdataArray;
            GraphXStringEDFragment<Long, Long, VD_T, ED_T> graphXFragment =
                    (GraphXStringEDFragment<Long, Long, VD_T, ED_T>) baseGraphXFragment;
            oldEDdataArray = graphXFragment.getEdataArray();
            oldVdataArray = graphXFragment.getVdataArray();
            logger.info(
                    "total bytes in vd array {}, vertices num {}",
                    oldVdataArray.getLength(),
                    graphXFragment.getVerticesNum());

            PrimitiveArray<VD_T> newVdataArray =
                    processPrimitiveArray(oldVdataArray, conf.getVdClass());
            PrimitiveArray<ED_T> newEdataArray =
                    processComplexArray(oldEDdataArray, conf.getEdClass());
            return new Tuple2<>(newVdataArray, newEdataArray);
        } else if (!conf.isVDPrimitive() && !conf.isEDPrimitive()) {
            StringTypedArray oldEDdataArray;
            StringTypedArray oldVdataArray;
            GraphXStringVEDFragment<Long, Long, VD_T, ED_T> graphXFragment =
                    (GraphXStringVEDFragment<Long, Long, VD_T, ED_T>) baseGraphXFragment;
            oldEDdataArray = graphXFragment.getEdataArray();
            oldVdataArray = graphXFragment.getVdataArray();
            logger.info(
                    "total bytes in vd array {}, total bytes in ed array {}, vertices num {}",
                    oldVdataArray.getLength(),
                    oldEDdataArray.getLength(),
                    graphXFragment.getVerticesNum());

            PrimitiveArray<VD_T> newVdataArray =
                    processComplexArray(oldVdataArray, conf.getVdClass());
            PrimitiveArray<ED_T> newEdataArray =
                    processComplexArray(oldEDdataArray, conf.getEdClass());
            return new Tuple2<>(newVdataArray, newEdataArray);
        } else {
            logger.error(
                    "Not implemented for vd {} ed {}", conf.isVDPrimitive(), conf.isEDPrimitive());
            throw new IllegalStateException("not implemented");
        }
    }

    public static <T> PrimitiveArray<T> processComplexArray(
            StringTypedArray oldArray, Class<? extends T> clz)
            throws IOException, ClassNotFoundException {
        FakeFFIByteVector vector =
                new FakeFFIByteVector(oldArray.getRawData(), oldArray.getRawDataLength());
        FakeFFIByteVectorInputStream ffiInput = new FakeFFIByteVectorInputStream(vector);
        long len = oldArray.getLength();
        logger.info("reading {} objects from array of bytes {}", len, oldArray.getLength());
        PrimitiveArray<T> newArray = PrimitiveArray.create(clz, (int) len);
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
            TypedArray<T> oldArray, Class<? extends T> clz) {
        return PrimitiveArray.createImmutable(oldArray, clz);
    }

    private static <T> PrimitiveArray<T> processPrimitiveArray(
            TypedArray<T> oldArray, Class<? extends T> clz) {
        PrimitiveArray<T> newArray = PrimitiveArray.create(clz, (int) oldArray.getLength());
        long time0 = System.nanoTime();
        long len = oldArray.getLength();
        for (int i = 0; i < len; ++i) {
            newArray.set(i, oldArray.get(i));
        }
        long time1 = System.nanoTime();
        return newArray;
    }

    private BaseGraphXFragment<Long, Long, VD, ED> getBaseGraphXFragment(
            IFragment<Long, Long, VD, ED> iFragment) {
        if (iFragment.fragmentType().equals(FragmentType.GraphXStringVDFragment)) {
            return ((GraphXStringVDFragmentAdaptor<Long, Long, VD, ED>) iFragment).getFragment();
        } else if (iFragment.fragmentType().equals(FragmentType.GraphXFragment)) {
            return ((GraphXFragmentAdaptor<Long, Long, VD, ED>) iFragment).getFragment();
        } else if (iFragment.fragmentType().equals(FragmentType.GraphXStringEDFragment)) {
            return ((GraphXStringEDFragmentAdaptor<Long, Long, VD, ED>) iFragment).getFragment();
        } else if (iFragment.fragmentType().equals(FragmentType.GraphXStringVEDFragment)) {
            return ((GraphXStringVEDFragmentAdaptor<Long, Long, VD, ED>) iFragment).getFragment();
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
