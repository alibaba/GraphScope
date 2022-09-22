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

import com.alibaba.graphscope.ds.TypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Function2;

import java.io.IOException;

public class IntMessageStore extends AbstractMessageStore<Integer> {

    private Logger logger = LoggerFactory.getLogger(IntMessageStore.class.getName());

    private AtomicIntegerArrayWrapper values;
    private Function2<Integer, Integer, Integer> mergeMessage;

    public IntMessageStore(
            int len,
            int fnum,
            int numCores,
            Function2<Integer, Integer, Integer> function2,
            ThreadSafeBitSet nextSet) {
        super(fnum, numCores, nextSet);
        values = new AtomicIntegerArrayWrapper(len);
        mergeMessage = function2;
    }

    @Override
    public Integer get(int index) {
        return values.get(index);
    }

    @Override
    public void set(int index, Integer value) {
        values.set(index, value);
    }

    @Override
    public int size() {
        return values.getSize();
    }

    @Override
    void threadSafeSet(int ind, Integer value) {
        values.compareAndSet(ind, value);
    }

    @Override
    void mergeAndSet(int ind, Integer value) {
        int original = get(ind);
        int newValue = mergeMessage.apply(original, value);
        values.compareAndSet(ind, newValue);
    }

    void writeMessageToStream(BaseGraphXFragment<Long, Long, ?, ?> fragment) throws IOException {
        int ivnum = (int) fragment.getInnerVerticesNum();
        int cnt = 0;
        Vertex<Long> vertex = tmpVertex[0];
        TypedArray<Long> outerLid2Gids = fragment.getVM().getOuterLid2GidAccessor();

        for (int i = nextSet.nextSetBit(ivnum); i >= 0; i = nextSet.nextSetBit(i + 1)) {
            vertex.SetValue((long) i);
            long outerGid = outerLid2Gids.get(i - ivnum);
            int dstFid = idParser.getFragId(outerGid);
            long dstLid = idParser.getLocalId(outerGid);
            outputStream[dstFid].writeLong(dstLid);
            outputStream[dstFid].writeInt(values.get(i));
            cnt += 1;
        }
        logger.debug("Frag [{}] try to send {} msg to outer vertices", fragment.fid(), cnt);
    }

    @Override
    public void digest(
            FFIByteVector vector,
            BaseGraphXFragment<Long, Long, ?, ?> fragment,
            ThreadSafeBitSet curSet) {
        FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(vector);
        int size = (int) vector.size();
        if (size <= 0) {
            throw new IllegalStateException("The received vector can not be empty");
        }

        try {
            while (inputStream.available() > 0) {
                int lid = (int) inputStream.readLong();
                int msg = inputStream.readInt();
                if (curSet.get(lid)) {
                    values.set(lid, mergeMessage.apply(values.get(lid), msg));
                } else {
                    values.set(lid, msg);
                    curSet.set(lid);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
