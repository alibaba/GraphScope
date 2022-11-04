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

package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.ds.DestList;
import com.alibaba.graphscope.ds.FidPointer;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.graphx.GraphXConf;
import com.alibaba.graphscope.graphx.graph.GSEdgeTripletImpl;
import com.alibaba.graphscope.graphx.utils.IdParser;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.collection.Iterator;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public abstract class AbstractMessageStore<T> implements MessageStore<T> {
    Logger logger = LoggerFactory.getLogger(AbstractMessageStore.class.getName());

    protected Vertex<Long> tmpVertex[];
    protected ThreadSafeBitSet nextSet;
    protected IdParser idParser;
    protected FFIByteVectorOutputStream[] outputStream;
    protected GraphXConf<?, ?, ?> conf;
    protected int ivnum;

    abstract void threadSafeSet(int ind, T value);

    abstract void mergeAndSet(int ind, T value);

    abstract void writeMessageToStream(IFragment<Long, Long, ?, ?> fragment) throws IOException;

    public AbstractMessageStore(
            int fnum, int numCores, ThreadSafeBitSet nextSet, GraphXConf<?, ?, ?> conf, int ivnum) {
        this.ivnum = ivnum;
        tmpVertex = new Vertex[numCores];
        for (int i = 0; i < numCores; ++i) {
            tmpVertex[i] = FFITypeFactoryhelper.newVertexLong();
        }
        outputStream = new FFIByteVectorOutputStream[fnum];
        for (int i = 0; i < fnum; ++i) {
            outputStream[i] = new FFIByteVectorOutputStream();
        }
        idParser = new IdParser(fnum);
        this.nextSet = nextSet;
        this.conf = conf;
    }

    /**
     * we assume this function is thread safe
     */
    @Override
    public <MSG_T> void sendMsgThroughIEdges(
            Vertex<Long> vertex,
            MSG_T msg,
            int threadId,
            ParallelMessageManager messageManager,
            IFragment<Long, Long, ?, ?> fragment) {
        Class<? extends MSG_T> clz = (Class<? extends MSG_T>) msg.getClass();
        if (clz.equals(Double.class) || clz.equals(Long.class) || clz.equals(Integer.class)) {
            messageManager.sendMsgThroughIEdges(fragment, vertex, msg, threadId);
        } else {
            if (!fragment.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
                throw new IllegalStateException("not supported for non-projected fragment");
            } else {
                if (vertex.getValue() % 10000 == 1) {
                    logger.info("send complex msg");
                }
                ArrowProjectedFragment<Long, Long, ?, ?> baseFrag =
                        ((ArrowProjectedAdaptor<Long, Long, ?, ?>) fragment)
                                .getArrowProjectedFragment();
                DestList destList = baseFrag.ieDestList(vertex);
                FFIByteVectorOutputStream stream = new FFIByteVectorOutputStream();
                try {
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(stream);
                    objectOutputStream.writeLong(fragment.getInnerVertexGid(vertex));
                    objectOutputStream.writeObject(msg);
                    objectOutputStream.flush();
                    stream.finishSetting();
                    for (FidPointer fidPointer : destList) {
                        int fid = fidPointer.get();
                        messageManager.sendToFragment(fid, stream.getVector(), threadId);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void addMessages(
            Iterator<Tuple2<Long, T>> msgs,
            int threadId,
            GSEdgeTripletImpl edgeTriplet,
            IFragment<Long, Long, ?, ?> fragment,
            int srcLid,
            int dstLid)
            throws InterruptedException {
        while (msgs.hasNext()) {
            Tuple2<Long, T> msg = msgs.next();
            // the oid must from src or dst, we first find with lid.
            long oid = msg._1();
            int lid;
            if (oid == edgeTriplet.dstId()) {
                lid = dstLid;
            } else if (oid == edgeTriplet.srcId()) {
                lid = srcLid;
            } else {
                logger.warn(
                        "sending msg to neither source or dst {}->{}, {}",
                        edgeTriplet.srcId(),
                        edgeTriplet.dstId(),
                        oid);
                Vertex<Long> vertex = tmpVertex[threadId];
                if (!fragment.getVertex(oid, vertex)) {
                    throw new IllegalStateException("Error in get vertex for oid " + oid);
                }
                lid = Math.toIntExact(vertex.getValue());
            }
            if (nextSet.get(lid)) {
                mergeAndSet(lid, msg._2());
            } else {
                threadSafeSet(lid, msg._2());
                nextSet.set(lid);
            }
            if (lid > ivnum) {
                logger.info(
                        "add message to outer vertex {}, ivnum {}, msg {}", lid, ivnum, msg._2());
            }
        }
    }

    @Override
    public void flushMessages(
            ThreadSafeBitSet nextSet,
            ParallelMessageManager messageManager,
            IFragment<Long, Long, ?, ?> fragment,
            int[] fid2WorkerId,
            ExecutorService executorService)
            throws IOException {
        CountDownLatch countDownLatch = new CountDownLatch(fragment.fnum());
        writeMessageToStream(fragment);
        for (int i = 0; i < outputStream.length; ++i) {
            final int tid = i;
            executorService.execute(
                    () -> {
                        outputStream[tid].finishSetting();
                        if (outputStream[tid].getVector().size() > 0) {
                            int workerId = fid2WorkerId[tid];
                            messageManager.sendToFragment(
                                    workerId, outputStream[tid].getVector(), tid);
                            logger.info(
                                    "fragment [{}] send {} bytes to [{}]",
                                    fragment.fid(),
                                    outputStream[tid].getVector().size(),
                                    tid);
                        }
                        outputStream[tid].reset();
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
}
