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

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.graphx.graph.GSEdgeTripletImpl;
import com.alibaba.graphscope.graphx.utils.IdParser;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;

import scala.Tuple2;
import scala.collection.Iterator;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public abstract class AbstractMessageStore<T> implements MessageStore<T> {

    protected Vertex<Long> tmpVertex[];
    protected ThreadSafeBitSet nextSet;
    protected IdParser idParser;
    protected FFIByteVectorOutputStream[] outputStream;

    abstract void threadSafeSet(int ind, T value);

    abstract void mergeAndSet(int ind, T value);

    abstract void writeMessageToStream(BaseGraphXFragment<Long, Long, ?, ?> fragment)
            throws IOException;

    public AbstractMessageStore(int fnum, int numCores, ThreadSafeBitSet nextSet) {
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
    }

    @Override
    public void addMessages(
            Iterator<Tuple2<Long, T>> msgs,
            BaseGraphXFragment<Long, Long, ?, ?> fragment,
            int threadId,
            GSEdgeTripletImpl edgeTriplet,
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
            } else {
                lid = srcLid;
            }
            if (nextSet.get(lid)) {
                mergeAndSet(lid, msg._2());
            } else {
                threadSafeSet(lid, msg._2());
                nextSet.set(lid);
            }
        }
    }

    @Override
    public void flushMessages(
            ThreadSafeBitSet nextSet,
            ParallelMessageManager messageManager,
            BaseGraphXFragment<Long, Long, ?, ?> fragment,
            int[] fid2WorkerId,
            ExecutorService executorService)
            throws IOException {
        CountDownLatch countDownLatch = new CountDownLatch(fragment.fnum());
        writeMessageToStream(fragment);
        for (int i = 0; i < fragment.fnum(); ++i) {
            final int tid = i;
            executorService.execute(
                    () -> {
                        if (tid != fragment.fid()) {
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
}
