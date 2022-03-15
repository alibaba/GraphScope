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

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * Specialized store for double msgs.
 *
 * @param <OID_T>
 */
public class LongLongMessageStore<OID_T extends WritableComparable>
        implements MessageStore<OID_T, LongWritable, Long> {

    private static Logger logger = LoggerFactory.getLogger(LongLongMessageStore.class);
    private static int INIT_CAPACITY = 2;

    private IFragment<?, Long, ?, ?> fragment;
    private ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf;
    private Vertex<Long> vertex;
    /**
     * lid 2 messages
     */
    private Long2LongOpenHashMap messages;

    private LongWritableIterable iterable;
    private long innerVerticesNum;

    public LongLongMessageStore(
            IFragment fragment, ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf) {
        this.fragment = fragment;
        this.conf = conf;
        vertex = (Vertex<Long>) FFITypeFactoryhelper.newVertex(java.lang.Long.class);
        iterable = new LongWritableIterable();
        messages = new Long2LongOpenHashMap((int) fragment.getInnerVerticesNum());
        innerVerticesNum = fragment.getInnerVerticesNum();
    }

    @Override
    public void addLidMessage(Long lid, LongWritable writable) {
        if (lid >= innerVerticesNum) {
            throw new IllegalStateException("lid exceeded upper bound");
        }
        messages.put((long) lid, writable.get());
    }

    public void addLidMessage(Long lid, long msg) {
        if (lid >= innerVerticesNum) {
            throw new IllegalStateException("lid exceeded upper bound");
        }
        messages.put((long) lid, msg);
    }

    @Override
    public void addGidMessages(
            Iterator<Long> gidIterator, Iterator<LongWritable> writableIterator) {
        int cnt = 0;
        while (gidIterator.hasNext() && writableIterator.hasNext()) {
            long gid = gidIterator.next();
            LongWritable msg = writableIterator.next();
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
    public synchronized void addGidMessage(Long gid, LongWritable writable) {
        addGidMessage0((Long) gid, writable.get());
    }

    private synchronized void addGidMessage0(Long gid, long msg) {
        if (!fragment.innerVertexGid2Vertex(gid, vertex)) {
            throw new IllegalStateException("gid to vertex convertion failed: " + gid);
        }
        long lid = vertex.GetValue();
        addLidMessage(lid, msg);
    }

    /**
     * For input byteBuf, parse and update our store.
     *
     * @param buf
     */
    public void digestByteBuf(ByteBuf buf) {
        buf.skipBytes(5);
        if (buf.readableBytes() % 16 != 0) {
            throw new IllegalStateException("Expect number of bytes times of 16");
        }
        logger.debug(
                "LongLongMsgStore digest bytebuf size {} direct {}",
                buf.readableBytes(),
                buf.isDirect());
        while (buf.readableBytes() >= 16) {
            long gid = buf.readLong();
            long msg = buf.readLong();
            addGidMessage0(gid, msg);
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
    public void swap(MessageStore<OID_T, LongWritable, Long> other) {
        if (other instanceof LongLongMessageStore) {
            LongLongMessageStore<OID_T> longLongMessageStore = (LongLongMessageStore<OID_T>) other;
            if (!this.fragment.equals(longLongMessageStore.fragment)) {
                logger.error("fragment not the same");
                return;
            }
            Long2LongOpenHashMap tmp;
            if (logger.isDebugEnabled()) {
                logger.debug("Before swap {} vs {}", this.messages, longLongMessageStore.messages);
            }
            tmp = this.messages;
            this.messages = longLongMessageStore.messages;
            longLongMessageStore.messages = tmp;
            if (logger.isDebugEnabled()) {
                logger.debug("After swap {} vs {}", this.messages, longLongMessageStore.messages);
            }
        } else {
            throw new IllegalStateException("Can not swap with a non-longLongMessageStore obj");
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
    public Iterable<LongWritable> getMessages(Long lid) {
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
            throw new IllegalStateException(
                    "Expect number of bytes times of 16 + 8, actual: " + size);
        }
        try {
            long dataSize = inputStream.readLong();
            if (dataSize != size - 8) {
                throw new IllegalStateException(
                        "Message size not match: vec size"
                                + size
                                + ", first field in vec"
                                + dataSize);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.debug("LongLongMsgStore digest FFIVector size {}", size);
        try {
            while (inputStream.longAvailable() >= 16) {
                long gid = inputStream.readLong();
                long msg = inputStream.readLong();
                addGidMessage0(gid, msg);
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

    public static class LongWritableIterable implements Iterable<LongWritable> {

        boolean res = true;
        private LongWritable writable;
        private Iterator<LongWritable> iterator =
                new Iterator<LongWritable>() {

                    @Override
                    public boolean hasNext() {
                        return res;
                    }

                    @Override
                    public LongWritable next() {
                        res = false;
                        return writable;
                    }
                };

        public LongWritableIterable() {
            writable = new LongWritable();
        }

        public void init(long in) {
            writable.set(in);
            res = true;
        }

        @Override
        public Iterator<LongWritable> iterator() {
            // Return a single iterator rather than creating for each call;
            return iterator;
        }
    }
}
