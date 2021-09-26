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
package com.alibaba.graphscope.parallel.message;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import io.netty.buffer.ByteBuf;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Specialized store for double msgs.
 *
 * @param <OID_T>
 */
public class LongDoubleArrayMessageStore<OID_T extends WritableComparable>
        implements MessageStore<OID_T, DoubleWritable, Long> {

    private static Logger logger = LoggerFactory.getLogger(LongDoubleMessageStore.class);
    private static int INIT_CAPACITY = 2;

    private IFragment<?, Long, ?, ?> fragment;
    private ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf;
    private Vertex<Long> vertex;
    /**
     * lid 2 messages
     */
    private Map<Long, List<Double>> messages;

    private DoubleWritableIterable iterable;
    private long innerVerticesNum;

    public LongDoubleArrayMessageStore(
            IFragment fragment, ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf) {
        this.fragment = fragment;
        this.conf = conf;
        vertex = (Vertex<Long>) FFITypeFactoryhelper.newVertex(java.lang.Long.class);
        iterable = new DoubleWritableIterable();
        messages = new HashMap<Long, List<Double>>((int) fragment.getInnerVerticesNum());
        innerVerticesNum = fragment.getInnerVerticesNum();
    }

    @Override
    public void addLidMessage(Long lid, DoubleWritable writable) {
        if (lid >= innerVerticesNum) {
            throw new IllegalStateException("lid exceeded upper bound");
        }
        if (!messages.containsKey(lid)) {
            messages.put(lid, new ArrayList<>(INIT_CAPACITY));
        }
        messages.get(lid).add(writable.get());
    }

    @Override
    public void addGidMessages(
            Iterator<Long> gidIterator, Iterator<DoubleWritable> writableIterator) {
        int cnt = 0;
        while (gidIterator.hasNext() && writableIterator.hasNext()) {
            long gid = gidIterator.next();
            DoubleWritable msg = writableIterator.next();
            if (!fragment.innerVertexGid2Vertex(gid, vertex)) {
                throw new IllegalStateException("gid to vertex convertion failed: " + gid);
            }
            long lid = vertex.GetValue();
            addLidMessage(lid, msg);
            cnt += 1;
        }
        logger.info("worker [{}] messages to self cnt: {}", fragment.fid(), cnt);
    }

    /**
     * For messages bound with gid, first get lid.
     *
     * @param gid      global id
     * @param writable msg
     */
    @Override
    public synchronized void addGidMessage(Long gid, DoubleWritable writable) {
        addGidMessage(gid, writable.get());
    }

    private synchronized void addGidMessage(Long gid, double msg) {
        if (!fragment.innerVertexGid2Vertex(gid, vertex)) {
            throw new IllegalStateException("gid to vertex convertion failed: " + gid);
        }
        long lid = vertex.GetValue();
        if (lid >= innerVerticesNum) {
            throw new IllegalStateException("exceeded innerVertices num");
        }
        if (!messages.containsKey(lid)) {
            messages.put(lid, new ArrayList<>());
        }
        messages.get(lid).add(msg);
    }

    /**
     * For input byteBuf, parse and update our store.
     *
     * <p>The received buf contains 4+1+data.
     *
     * @param buf
     */
    public void digestByteBuf(ByteBuf buf) {
        // FIXME: why we are copying?
        buf.skipBytes(5);
        if (buf.readableBytes() % 16 != 0) {
            throw new IllegalStateException("Expect number of bytes times of 16");
        }
        logger.debug(
                "LongDoubleMsgStore digest bytebuf size {} direct {}",
                buf.readableBytes(),
                buf.isDirect());
        while (buf.readableBytes() >= 16) {
            long gid = buf.readLong();
            double msg = buf.readDouble();
            addGidMessage(gid, msg);
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "worker [{}] resolving message to self, gid {}, msg {}",
                        fragment.fid(),
                        gid,
                        msg);
            }
        }
        if (buf.readableBytes() != 0) {
            throw new IllegalStateException("readable bytes no subtracted by 16");
        }
    }

    @Override
    public void swap(MessageStore<OID_T, DoubleWritable, Long> other) {
        if (other instanceof LongDoubleArrayMessageStore) {
            LongDoubleArrayMessageStore<OID_T> longDoubleMessageStore =
                    (LongDoubleArrayMessageStore<OID_T>) other;
            if (!this.fragment.equals(longDoubleMessageStore.fragment)) {
                logger.error("fragment not the same");
                return;
            }
            Map<Long, List<Double>> tmp;
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Before swap {} vs {}", this.messages, longDoubleMessageStore.messages);
            }
            tmp = this.messages;
            this.messages = longDoubleMessageStore.messages;
            longDoubleMessageStore.messages = tmp;
            if (logger.isDebugEnabled()) {
                logger.debug("After swap {} vs {}", this.messages, longDoubleMessageStore.messages);
            }
        } else {
            logger.error("Can not swap with a non-longDoubleMessageStore obj");
        }
    }

    @Override
    public void clearAll() {
        messages.clear();
    }

    /**
     * Check whether any messages received.
     */
    @Override
    public boolean anyMessageReceived() {
        return !messages.isEmpty();
    }

    /**
     * Check for lid, any messages available.
     *
     * @param lid lid.
     * @return true if has message
     */
    @Override
    public boolean messageAvailable(Long lid) {
        if (lid >= innerVerticesNum) {
            throw new IllegalStateException("lid exceeded upper bound");
        }
        return messages.containsKey(lid);
    }

    /**
     * Avoid creating messages, this function is not thread-safe.
     *
     * @param lid
     * @return
     */
    @Override
    public Iterable<DoubleWritable> getMessages(Long lid) {
        if (lid >= innerVerticesNum) {
            throw new IllegalStateException("lid exceeded upper bound");
        }
        if (messages.containsKey(lid)) {
            iterable.init(messages.get(lid));
            return iterable;
        } else {
            // actually a static empty iterator.
            return () -> Collections.emptyIterator();
        }
    }

    /**
     * For a bytestream provided by FFIByteVector, read from it and digest its content.
     *
     * @param vector
     */
    @Override
    public void digest(FFIByteVector vector) {
        FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(vector);
        int size = (int) vector.size();
        if (size <= 0) {
            return;
        }
        if (size % 16 != 8) {
            throw new IllegalStateException("Expect number of bytes times of 16");
        }
        logger.debug("LongDoubleMsgStore digest FFIVector size {}", size);
        try {
            long expectSize = inputStream.readLong();
            if (expectSize != inputStream.longAvailable()) {
                throw new IllegalStateException(
                        "Expect bytes "
                                + expectSize
                                + " available: "
                                + inputStream.longAvailable());
            }
            while (inputStream.longAvailable() >= 16) {
                long gid = inputStream.readLong();
                double msg = inputStream.readDouble();
                addGidMessage(gid, msg);
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "worker [{}] resolving message to self, gid {}, msg {}",
                            fragment.fid(),
                            gid,
                            msg);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (inputStream.longAvailable() != 0) {
            throw new IllegalStateException("readable bytes no subtracted by 16");
        }
    }

    public static class DoubleWritableIterable implements Iterable<DoubleWritable> {

        private List<Double> doubles;
        private int ind;
        private DoubleWritable writable;

        private Iterator<DoubleWritable> iterator =
                new Iterator<DoubleWritable>() {

                    @Override
                    public boolean hasNext() {
                        return ind < doubles.size();
                    }

                    @Override
                    public DoubleWritable next() {
                        writable.set(doubles.get(ind++));
                        return writable;
                    }
                };

        public DoubleWritableIterable() {
            doubles = new ArrayList<>();
            ind = 0;
            writable = new DoubleWritable();
        }

        public void init(List<Double> in) {
            doubles = in;
            ind = 0;
        }

        @Override
        public Iterator<DoubleWritable> iterator() {
            // Return a single iterator rather than creating for each call;
            return iterator;
        }
    }
}
