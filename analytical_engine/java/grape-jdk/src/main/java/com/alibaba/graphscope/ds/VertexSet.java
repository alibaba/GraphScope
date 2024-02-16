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

import com.alibaba.fastffi.FFITypeFactory;

import java.util.BitSet;

/**
 * VertexSet Uses {@link BitSet} as underlying data structure to mark the presence of a vertex. It
 * provides interfaces which works along with {@link Vertex} and {@link VertexRange}. Feel free to
 * use VertexSet in your app.
 *
 * <p>Right index is exclusive We don't want to introduce synchronization cost, so the underlying
 * backend is a no-thread-safe bitset.
 *
 * <p>bitSize must be specified in concurrent situations. since expansion has no concurrency
 * control.
 *
 * @see DenseVertexSet
 */
public class VertexSet {
    //    private BitSet bs;
    private Bitset bs;
    private static Bitset.Factory factory = FFITypeFactory.getFactory("grape::Bitset");
    private long left;
    // right is exclusive
    private long right;

    public VertexSet(int start, int end) {
        left = start;
        right = end;
        bs = factory.create();
        bs.init(right - left);
    }

    public VertexSet(long start, long end) {
        left = start;
        right = end;
        bs = factory.create();
        bs.init(right - left);
    }

    public VertexSet(VertexRange<Long> vertices) {
        left = vertices.beginValue();
        right = vertices.endValue();
        bs = factory.create();
        bs.init(right - left);
    }

    public long count() {
        return bs.count();
    }

    public long getLeft() {
        return left;
    }

    public long getRight() {
        return right;
    }

    public Bitset getBitSet() {
        return bs;
    }

    public boolean exist(int vid) {
        return bs.getBit(vid - left);
    }

    public boolean get(int vid) {
        return bs.getBit(vid - left);
    }

    public boolean get(Vertex<Long> vertex) {
        return bs.getBit(vertex.getValue() - left);
    }

    public boolean get(long vid) {
        return bs.getBit(vid - left);
    }

    public void set(int vid) {
        bs.setBit(vid - left);
    }

    /**
     * This function is not thread safe, even you are assigning threads with segmented partition.
     * Because java {@code Bitset} init the {@code wordinUse = 0} and adaptively increases it,
     * {@code ensureCapacity} is to be invoked, causing problem. So we access the highest bit in
     * initializing.
     *
     * @param vertex input vertex.
     */
    public void set(Vertex<Long> vertex) {
        bs.setBit(vertex.getValue() - left);
    }

    /**
     * Bind a vertex with a new vertex data.
     *
     * @param vertex querying vertex.
     * @param newValue value to be bound to querying vertex.
     */
    public void set(Vertex<Long> vertex, boolean newValue) {
        if (newValue) {
            bs.setBit(vertex.getValue() - left);
        } else {
            bs.resetBit(vertex.getValue());
        }
    }

    public void set(long vid) {
        bs.setBit(vid - left);
    }

    public boolean empty() {
        return bs.count() <= 0;
    }

    /**
     * Check whether all [l, r) values are false;
     *
     * @param l left boundary.
     * @param r right boundary.
     * @return empty in this range or not.
     */
    public boolean partialEmpty(int l, int r) {
        for (long i = l; i < r; ++i) {
            if (bs.getBit(i)) return false;
        }
        return true;
    }

    public void insert(Vertex<Long> vertex) {
        bs.getBit(vertex.getValue());
    }

    /** Erase current status. */
    public void clear() {
        bs.clear();
    }

    /**
     * Set this vertex set with bits from another.
     *
     * @param other Another vertex set
     */
    public void assign(VertexSet other) {
        Bitset otherBitset = other.bs;
        bs.copy(otherBitset);
    }
}
