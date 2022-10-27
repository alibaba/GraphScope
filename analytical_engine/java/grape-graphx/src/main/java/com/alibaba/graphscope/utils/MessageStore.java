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
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graphx.GraphXConf;
import com.alibaba.graphscope.graphx.graph.GSEdgeTripletImpl;
import com.alibaba.graphscope.parallel.MessageInBuffer;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.utils.array.PrimitiveArray;

import scala.Function2;
import scala.Tuple2;
import scala.collection.Iterator;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * Message store with bitset indicating validity.
 *
 * @param <T> type
 */
public interface MessageStore<T> extends PrimitiveArray<T> {
    /** Send msg through iedges, use this function rather than message manager provided to speed up. The MSG_T can be different from T */
    <MSG_T> void sendMsgThroughIEdges(
            Vertex<Long> vertex,
            MSG_T msg,
            int threadId,
            ParallelMessageManager messageManager,
            IFragment<Long, Long, ?, ?> fragment);

    void addMessages(
            Iterator<Tuple2<Long, T>> msgs,
            int threadId,
            GSEdgeTripletImpl triplet,
            IFragment<Long, Long, ?, ?> fragment,
            int srcLid,
            int dstLid)
            throws InterruptedException;

    void flushMessages(
            ThreadSafeBitSet nextSet,
            ParallelMessageManager messageManager,
            IFragment<Long, Long, ?, ?> fragment,
            int[] fid2WorkerId,
            ExecutorService executorService)
            throws IOException;

    void digest(
            IFragment<Long, Long, ?, ?> fragment,
            FFIByteVector vector,
            ThreadSafeBitSet curSet,
            int threadId);

    /** to digest message send along edges, which should be resolved via GetMessages.*/
    long digest(
            IFragment<Long, Long, ?, ?> fragment,
            MessageInBuffer messageInBuffer,
            ThreadSafeBitSet curSet,
            int threadId);

    static <T> MessageStore<T> create(
            int len,
            int fnum,
            int numCores,
            Class<? extends T> clz,
            Function2<T, T, T> function2,
            ThreadSafeBitSet nextSet,
            GraphXConf<?, ?, ?> conf,
            int ivnum)
            throws IOException {
        if (clz.equals(Long.class) || clz.equals(long.class)) {
            return (MessageStore<T>)
                    new LongMessageStore(
                            len,
                            fnum,
                            numCores,
                            ivnum,
                            (Function2<Long, Long, Long>) function2,
                            nextSet,
                            conf);
        } else if (clz.equals(Double.class) || clz.equals(double.class)) {
            return (MessageStore<T>)
                    new DoubleMessageStore(
                            len,
                            fnum,
                            numCores,
                            ivnum,
                            (Function2<Double, Double, Double>) function2,
                            nextSet,
                            conf);
        } else if (clz.equals(Integer.class) || clz.equals(int.class)) {
            return (MessageStore<T>)
                    new IntMessageStore(
                            len,
                            fnum,
                            numCores,
                            ivnum,
                            (Function2<Integer, Integer, Integer>) function2,
                            nextSet,
                            conf);
        } else {
            return new ObjectMessageStore<>(
                    len, fnum, numCores, ivnum, clz, function2, nextSet, conf);
        }
    }
}
