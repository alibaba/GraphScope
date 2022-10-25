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

import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.graphx.graph.GSEdgeTripletImpl;
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
 * @param <T> type
 */
public interface MessageStore<T> extends PrimitiveArray<T> {

    void addMessages(
            Iterator<Tuple2<Long, T>> msgs,
            BaseGraphXFragment<Long, Long, ?, ?> fragment,
            int threadId,
            GSEdgeTripletImpl triplet,
            int srcLid,
            int dstLid)
            throws InterruptedException;

    void flushMessages(
            ThreadSafeBitSet nextSet,
            ParallelMessageManager messageManager,
            BaseGraphXFragment<Long, Long, ?, ?> fragment,
            int[] fid2WorkerId,
            ExecutorService executorService)
            throws IOException;

    void digest(
            FFIByteVector vector,
            BaseGraphXFragment<Long, Long, ?, ?> fragment,
            ThreadSafeBitSet curSet);

    static <T> MessageStore<T> create(
            int len,
            int fnum,
            int numCores,
            Class<? extends T> clz,
            Function2<T, T, T> function2,
            ThreadSafeBitSet nextSet)
            throws IOException {
        if (clz.equals(Long.class) || clz.equals(long.class)) {
            return (MessageStore<T>)
                    new LongMessageStore(
                            len, fnum, numCores, (Function2<Long, Long, Long>) function2, nextSet);
        } else if (clz.equals(Double.class) || clz.equals(double.class)) {
            return (MessageStore<T>)
                    new DoubleMessageStore(
                            len,
                            fnum,
                            numCores,
                            (Function2<Double, Double, Double>) function2,
                            nextSet);
        } else if (clz.equals(Integer.class) || clz.equals(int.class)) {
            return (MessageStore<T>)
                    new IntMessageStore(
                            len,
                            fnum,
                            numCores,
                            (Function2<Integer, Integer, Integer>) function2,
                            nextSet);
        } else return new ObjectMessageStore<>(len, fnum, numCores, clz, function2, nextSet);
    }
}
