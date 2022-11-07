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
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Function2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ExecutorService;

public class ObjectMessageStore<T> extends AbstractMessageStore<T> {

    private Logger logger = LoggerFactory.getLogger(ObjectMessageStore.class.getName());

    private AtomicObjectArrayWrapper<T> values;
    private Class<? extends T> clz;
    private Function2<T, T, T> mergeMessage;
    private ObjectOutputStream[] objectOutputStreams;

    public ObjectMessageStore(
            int len,
            int fnum,
            int numCores,
            int ivnum,
            Class<? extends T> clz,
            Function2<T, T, T> function2,
            ThreadSafeBitSet nextSet,
            GraphXConf<?, ?, ?> conf) {
        super(fnum, numCores, nextSet, conf, ivnum);
        this.clz = clz;
        values = new AtomicObjectArrayWrapper<>(len);
        mergeMessage = function2;
        objectOutputStreams = new ObjectOutputStream[fnum];
        for (int i = 0; i < fnum; ++i) {
            try {
                objectOutputStreams[i] = new ObjectOutputStream(outputStream[i]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public T get(int index) {
        return values.get(index);
    }

    @Override
    public void set(int index, T value) {
        values.set(index, value);
    }

    @Override
    public int size() {
        return values.getSize();
    }

    @Override
    void threadSafeSet(int ind, T value) {
        values.compareAndSet(ind, value);
    }

    @Override
    void mergeAndSet(int ind, T value) {
        T original = get(ind);
        T newValue = mergeMessage.apply(original, value);
        values.compareAndSet(ind, newValue);
    }

    @Override
    void writeMessageToStream(IFragment<Long, Long, ?, ?> fragment) throws IOException {
        int ivnum = (int) fragment.getInnerVerticesNum();
        int cnt = 0;
        Vertex<Long> vertex = tmpVertex[0];

        for (int i = nextSet.nextSetBit(ivnum); i >= 0; i = nextSet.nextSetBit(i + 1)) {
            vertex.setValue((long) i);
            long outerGid = fragment.getOuterVertexGid(vertex);
            int dstFid = idParser.getFragId(outerGid);
            //            long dstLid = idParser.getLocalId(outerGid);
            objectOutputStreams[dstFid].writeLong(outerGid);
            objectOutputStreams[dstFid].writeObject(values.get(i));
            cnt += 1;
        }
        logger.debug("Frag [{}] try to send {} msg to outer vertices", fragment.fid(), cnt);
    }

    @Override
    public void flushMessages(
            ThreadSafeBitSet nextSet,
            ParallelMessageManager messageManager,
            IFragment<Long, Long, ?, ?> fragment,
            int[] fid2WorkerId,
            ExecutorService executorService)
            throws IOException {
        // finish stream
        for (int i = 0; i < fragment.fnum(); ++i) {
            if (i != fragment.fid()) {
                objectOutputStreams[i].flush();
            }
        }
        super.flushMessages(nextSet, messageManager, fragment, fid2WorkerId, executorService);
        for (int i = 0; i < fragment.fnum(); ++i) {
            if (i != fragment.fid()) {
                objectOutputStreams[i] = new ObjectOutputStream(outputStream[i]);
            }
        }
    }

    @Override
    public void digest(
            IFragment<Long, Long, ?, ?> fragment,
            FFIByteVector vector,
            //            BaseGraphXFragment<Long, Long, ?, ?> fragment,
            ThreadSafeBitSet curSet,
            int threadId) {
        ObjectInputStream inputStream = null;
        try {
            inputStream = new ObjectInputStream(new FFIByteVectorInputStream(vector));
        } catch (IOException e) {
            e.printStackTrace();
        }
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
                int lid = Math.toIntExact(vertex.getValue());
                T msg = (T) inputStream.readObject();
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
        throw new IllegalStateException("Current not supported");
    }
}
