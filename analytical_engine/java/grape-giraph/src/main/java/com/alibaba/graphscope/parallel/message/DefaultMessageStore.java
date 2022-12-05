/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.parallel.message;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
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

public class DefaultMessageStore<OID_T extends WritableComparable, MSG_T extends Writable, GS_VID_T>
        implements MessageStore<OID_T, MSG_T, GS_VID_T> {

    private static Logger logger = LoggerFactory.getLogger(DefaultMessageStore.class);
    private static int INIT_CAPACITY = 2;

    private IFragment<?, GS_VID_T, ?, ?> fragment;
    private ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf;
    private Vertex<GS_VID_T> vertex;
    /**
     * lid 2 messages
     */
    private Map<GS_VID_T, List<MSG_T>> messages;

    private int innerVerticesNum;
    private int vid_t;

    public DefaultMessageStore(
            IFragment<?, GS_VID_T, ?, ?> fragment,
            ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf) {
        this.fragment = fragment;
        this.conf = conf;
        vertex = (Vertex<GS_VID_T>) FFITypeFactoryhelper.newVertex(conf.getGrapeVidClass());
        //        iterable = new DoubleWritableIterable();
        innerVerticesNum = (int) fragment.getInnerVerticesNum();
        messages = new HashMap<GS_VID_T, List<MSG_T>>(innerVerticesNum);
        if (conf.getGrapeVidClass().equals(Long.class)) {
            vid_t = 0;
        } else if (conf.getGrapeVidClass().equals(Integer.class)) {
            vid_t = 1;
        } else {
            throw new IllegalStateException("Grape vid should be long or integer");
        }
    }

    @Override
    public void addLidMessage(GS_VID_T lid, MSG_T writable) {
        if (!messages.containsKey(lid)) {
            messages.put(lid, Lists.newArrayListWithCapacity(INIT_CAPACITY));
        }
        messages.get(lid).add(writable);
    }

    @Override
    public void addGidMessages(Iterator<GS_VID_T> gids, Iterator<MSG_T> writables) {
        int cnt = 0;
        while (gids.hasNext() && writables.hasNext()) {
            GS_VID_T gid = gids.next();
            MSG_T msg = writables.next();
            if (!fragment.innerVertexGid2Vertex(gid, vertex)) {
                throw new IllegalStateException("gid to vertex convertion failed: " + gid);
            }
            GS_VID_T lid = vertex.getValue();
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
    public void addGidMessage(GS_VID_T gid, MSG_T writable) {
        if (!fragment.innerVertexGid2Vertex(gid, vertex)) {
            throw new IllegalStateException("gid to vertex convertion failed: " + gid);
        }
        GS_VID_T lid = vertex.getValue();
        if (!messages.containsKey(lid)) {
            messages.put(lid, new ArrayList<>());
        }
        messages.get(lid).add(writable);
    }

    @Override
    public void swap(MessageStore<OID_T, MSG_T, GS_VID_T> other) {
        if (other instanceof DefaultMessageStore) {
            DefaultMessageStore<OID_T, MSG_T, GS_VID_T> otherStore =
                    (DefaultMessageStore<OID_T, MSG_T, GS_VID_T>) other;
            if (!this.fragment.equals(otherStore.fragment)) {
                logger.error("fragment not the same");
                return;
            }
            Map<GS_VID_T, List<MSG_T>> tmp;
            if (logger.isDebugEnabled()) {
                logger.debug("Before swap {} vs {}", this.messages, otherStore.messages);
            }
            tmp = this.messages;
            this.messages = otherStore.messages;
            otherStore.messages = tmp;
            if (logger.isDebugEnabled()) {
                logger.debug("After swap {} vs {}", this.messages, otherStore.messages);
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
    public boolean messageAvailable(GS_VID_T lid) {
        return messages.containsKey(lid);
    }

    @Override
    public boolean messageAvailable(Long lid) {
        if (vid_t == 0) {
            return messages.containsKey(lid);
        } else {
            return messages.containsKey(((Number) lid).intValue());
        }
    }

    @Override
    public Iterable<MSG_T> getMessages(Long lid) {
        if (vid_t == 0) {
            return getMessages((GS_VID_T) lid);
        } else {
            return getMessages((GS_VID_T) (Integer) ((Number) lid).intValue());
        }
    }

    @Override
    public Iterable<MSG_T> getMessages(GS_VID_T lid) {
        if (messages.containsKey(lid)) {
            return messages.get(lid);
        }
        return () -> Collections.emptyIterator();
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

        logger.debug("DefaultMessageStore digest FFIVector size {}", size);
        try {
            long expectSize = inputStream.readLong();
            if (expectSize != inputStream.longAvailable()) {
                throw new IllegalStateException(
                        "Expect bytes "
                                + expectSize
                                + " available: "
                                + inputStream.longAvailable());
            }
            while (inputStream.longAvailable() > 0) {
                GS_VID_T gid;
                switch (vid_t) {
                    case 0:
                        gid = (GS_VID_T) (Long) inputStream.readLong();
                        break;
                    case 1:
                        gid = (GS_VID_T) (Integer) inputStream.readInt();
                        break;
                    default:
                        throw new IllegalStateException("Unknown flag " + vid_t);
                }
                MSG_T msg = ReflectionUtils.newInstance(conf.getIncomingMessageValueClass());
                msg.readFields(inputStream);
                addGidMessage(gid, msg);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (inputStream.longAvailable() != 0) {
            throw new IllegalStateException(
                    "still " + inputStream.longAvailable() + " remains in stream");
        }
    }

    @Override
    public void digestByteBuffer(ByteBuf buf, boolean fromSelf) {
        ByteBufInputStream inputStream = new ByteBufInputStream(buf);
        if (fromSelf) {
            buf.skipBytes(5);
        }
        logger.debug(
                "DefaultMessageStore digest bytebuf size {} direct {}",
                buf.readableBytes(),
                buf.isDirect());
        try {
            while (buf.readableBytes() > 8) {
                GS_VID_T gid;
                switch (vid_t) {
                    case 0:
                        gid = (GS_VID_T) (Long) buf.readLong();
                        break;
                    case 1:
                        gid = (GS_VID_T) (Integer) buf.readInt();
                        break;
                    default:
                        throw new IllegalStateException("Unknown flag " + vid_t);
                }
                MSG_T msg = ReflectionUtils.newInstance(conf.getIncomingMessageValueClass());
                msg.readFields(inputStream);
                addGidMessage(gid, msg);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (buf.readableBytes() != 0) {
            throw new IllegalStateException("readable bytes no subtracted by 16");
        }
    }
}
