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

import com.alibaba.graphscope.stdcxx.FFIByteVector;

import io.netty.buffer.ByteBuf;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Iterator;

/**
 * Message store
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public interface MessageStore<I extends WritableComparable, M extends Writable, GS_VID_T> {

    void addLidMessage(GS_VID_T lid, M writable);

    void addGidMessages(Iterator<GS_VID_T> gids, Iterator<M> writables);

    /**
     * For messages bound with gid, first get lid.
     *
     * @param gid      global id
     * @param writable msg
     */
    void addGidMessage(GS_VID_T gid, M writable);

    void swap(MessageStore<I, M, GS_VID_T> other);

    void clearAll();

    /**
     * Check whether any messages received.
     */
    boolean anyMessageReceived();

    /**
     * Check for lid, any messages available.
     *
     * @param lid lid.
     * @return true if has message
     */
    boolean messageAvailable(GS_VID_T lid);

    boolean messageAvailable(Long lid);

    Iterable<M> getMessages(GS_VID_T lid);

    Iterable<M> getMessages(Long lid);

    /**
     * For a bytestream provided by FFIByteVector, read from it and digest its content.
     *
     * @param vector
     */
    void digest(FFIByteVector vector);

    void digestByteBuffer(ByteBuf buf, boolean fromSelf);
}
