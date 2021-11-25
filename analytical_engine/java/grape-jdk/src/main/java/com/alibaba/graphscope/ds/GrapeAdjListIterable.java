/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.ds;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * An experimental substitution for AdjList, which is implemented in a more efficient manner, by
 * avoiding jni invocations.
 *
 * @param <VID_T> vertex id type.
 * @param <EDATA_T> edge data type.
 */
public class GrapeAdjListIterable<VID_T, EDATA_T> implements Iterable<GrapeNbr<VID_T, EDATA_T>> {
    private GrapeNbr<VID_T, EDATA_T> beginNbr;
    private GrapeNbr<VID_T, EDATA_T> endNbr;
    private long endNbrAddr;
    private long elementSize;
    private long beginNbrAddr;

    public GrapeAdjListIterable(GrapeNbr<VID_T, EDATA_T> inbegin, GrapeNbr<VID_T, EDATA_T> inEnd) {
        beginNbr = inbegin;
        endNbr = inEnd;
        elementSize = inbegin.elementSize();
        endNbrAddr = endNbr.getAddress();
        beginNbrAddr = beginNbr.getAddress();
    }

    /**
     * Get the begin Nbr.
     *
     * @return the first Nbr.
     */
    public GrapeNbr<VID_T, EDATA_T> begin() {
        return beginNbr;
    }

    /**
     * Get the end Nbr.
     *
     * @return the last Nbr.
     */
    public GrapeNbr<VID_T, EDATA_T> end() {
        return endNbr;
    }

    /**
     * Get the number of neighbors in this adj list.
     *
     * @return The number of neighbors.
     */
    public long size() {
        return (endNbrAddr - beginNbrAddr) / elementSize;
    }

    /**
     * Get the iterator.
     *
     * @return the iterator.
     */
    public Iterator<GrapeNbr<VID_T, EDATA_T>> iterator() {
        return new Iterator<GrapeNbr<VID_T, EDATA_T>>() {
            private GrapeNbr<VID_T, EDATA_T> cur = begin().moveTo(beginNbrAddr);
            private long curAddr = beginNbrAddr;

            @Override
            public boolean hasNext() {
                return curAddr != endNbrAddr;
            }

            @Override
            public GrapeNbr<VID_T, EDATA_T> next() {
                cur.setAddress(curAddr);
                curAddr += elementSize;
                return cur;
            }
        };
    }

    public void forEachVertex(Consumer<GrapeNbr<VID_T, EDATA_T>> consumer) {
        GrapeNbr<VID_T, EDATA_T> cur = begin().moveTo(beginNbrAddr);
        while (cur.getAddress() != endNbrAddr) {
            consumer.accept(cur);
            cur.addV(elementSize);
        }
    }
}
