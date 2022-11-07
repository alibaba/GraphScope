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
import com.alibaba.graphscope.parallel.MessageInBuffer;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Function2;

import java.io.IOException;

public class DoubleMessageStore extends AbstractMessageStore<Double> {

    private Logger logger = LoggerFactory.getLogger(DoubleMessageStore.class.getName());

    private AtomicDoubleArrayWrapper values;
    private Function2<Double, Double, Double> mergeMessage;
    protected DoubleMsg[] msgWrappers;

    public DoubleMessageStore(
            int len,
            int fnum,
            int numCores,
            int ivnum,
            Function2<Double, Double, Double> function2,
            ThreadSafeBitSet nextSet,
            GraphXConf<?, ?, ?> conf) {
        super(fnum, numCores, nextSet, conf, ivnum);
        values = new AtomicDoubleArrayWrapper(len);
        mergeMessage = function2;
        msgWrappers = new DoubleMsg[numCores];
        for (int i = 0; i < numCores; ++i) {
            msgWrappers[i] = DoubleMsg.factory.create();
        }
    }

    @Override
    public Double get(int index) {
        return values.get(index);
    }

    @Override
    public void set(int index, Double value) {
        values.set(index, value);
    }

    @Override
    public int size() {
        return values.getSize();
    }

    @Override
    void threadSafeSet(int ind, Double value) {
        values.compareAndSet(ind, value);
    }

    @Override
    void mergeAndSet(int ind, Double value) {
        double original = get(ind);
        double newValue = mergeMessage.apply(original, value);
        values.compareAndSet(ind, newValue);
    }

    @Override
    void writeMessageToStream(IFragment<Long, Long, ?, ?> fragment) throws IOException {
        int ivnum = (int) fragment.getInnerVerticesNum();
        int cnt = 0;
        Vertex<Long> vertex = tmpVertex[0];
        logger.info(
                "Frag {} in writing msg to stream, ivnum {},  next of ivnum {}",
                fragment.fid(),
                ivnum,
                nextSet.nextSetBit(ivnum));
        for (int i = nextSet.nextSetBit(ivnum); i >= 0; i = nextSet.nextSetBit(i + 1)) {
            vertex.setValue((long) i);
            long outerGid = fragment.getOuterVertexGid(vertex);
            int dstFid = idParser.getFragId(outerGid);
            //            long dstLid = idParser.getLocalId(outerGid);
            outputStream[dstFid].writeLong(outerGid);
            outputStream[dstFid].writeDouble(values.get(i));
            cnt += 1;
        }
        logger.info("Frag [{}] try to send {} msg to outer vertices", fragment.fid(), cnt);
    }

    @Override
    public void digest(
            IFragment<Long, Long, ?, ?> fragment,
            FFIByteVector vector,
            ThreadSafeBitSet curSet,
            int threadId) {
        FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(vector);
        int size = (int) vector.size();
        if (size <= 0) {
            throw new IllegalStateException("The received vector can not be empty");
        }

        Vertex<Long> vertex = tmpVertex[threadId];
        try {
            while (inputStream.available() > 0) {
                long gid = inputStream.readLong();
                if (!fragment.gid2Vertex(gid, vertex)) {
                    throw new IllegalStateException("Error in gid 2 vertex conversion " + gid);
                }
                if (vertex.getValue() > fragment.getVerticesNum()) {
                    throw new IllegalStateException(
                            "received lid "
                                    + vertex.getValue()
                                    + " , greater than tvnum: "
                                    + fragment.getVerticesNum());
                }
                int lid = Math.toIntExact(vertex.getValue());
                double msg = inputStream.readDouble();
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

    @Override
    public long digest(
            IFragment<Long, Long, ?, ?> fragment,
            MessageInBuffer messageInBuffer,
            ThreadSafeBitSet curSet,
            int threadId) {

        Vertex<Long> myVertex = tmpVertex[threadId];
        DoubleMsg msg = msgWrappers[threadId];
        long msgCnt = 0;
        while (true) {
            boolean res = messageInBuffer.getMessage(fragment, myVertex, msg);
            if (!res) break;
            if (myVertex.getValue() > fragment.getVerticesNum()) {
                throw new IllegalStateException(
                        "received lid "
                                + myVertex.getValue()
                                + " , greater than tvnum: "
                                + fragment.getVerticesNum());
            }
            values.set(myVertex.getValue().intValue(), msg.getData());
            msgCnt += 1;
        }
        return msgCnt;
    }
}
