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

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_DENSE_VERTEX_SET;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_DENSE_VERTEX_SET_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_WORKER_COMM_SPEC_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

/**
 * Java wrapper for grape <a
 * href="https://github.com/alibaba/libgrape-lite/blob/master/grape/utils/vertex_set.h">DenseVertexSet</a>.
 * DenseVertexSet is able to maintain the indicator information for a vertex range.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(value = {GRAPE_WORKER_COMM_SPEC_H, GRAPE_DENSE_VERTEX_SET_H})
@FFITypeAlias(GRAPE_DENSE_VERTEX_SET)
public interface DenseVertexSet<VID_T> extends FFIPointer, CXXPointer {

    /**
     * Init the vertex set with a range of vertices.
     *
     * @param range vertex range
     */
    @FFINameAlias("Init")
    void init(@CXXReference VertexRange<VID_T> range);

    /**
     * Insert one vertex into the set.
     *
     * @param u inserted vertex.
     */
    @FFINameAlias("Insert")
    void insert(@CXXReference Vertex<VID_T> u);

    /**
     * Insert the vertex with return value.
     *
     * @param u inserted vertex.
     * @return true if this operation changes the corresponding bit, false if the vertex is already
     *     inserted.
     */
    @FFINameAlias("InsertWithRet")
    boolean insertWithRet(@CXXReference Vertex<VID_T> u);

    /**
     * Remove one vertex from this vertexSet.
     *
     * @param u vertex to remove.
     */
    @FFINameAlias("Erase")
    void erase(@CXXReference Vertex<VID_T> u);

    /**
     * Remove one vertex with return value.
     *
     * @param u vertex to remove.
     * @return true if this operation changes the corresponding bit, false if the vertex is already
     *     erased, or not in this set.
     */
    @FFINameAlias("EraseWithRet")
    boolean eraseWithRet(@CXXReference Vertex<VID_T> u);

    /**
     * Check whether a vertex exists in a vertex set.
     *
     * @param u vertex to check.
     * @return boolean indicates the vertex exist or not.
     */
    @FFINameAlias("Exist")
    boolean exist(@CXXReference Vertex<VID_T> u);

    /**
     * Get the range for vertices.
     *
     * @return the possible range for vertices in this vertex set.
     */
    @FFINameAlias("Range")
    @CXXValue
    VertexRange<VID_T> range();

    /**
     * Count the number of vertices inserted into this set.
     *
     * @return number of vertices in this set.
     */
    @FFINameAlias("Count")
    long count();

    /**
     * Count the number of vertices inserted in a specified range.
     *
     * @param beg begin vertex id(inclusive).
     * @param end end vertex id(exclusive).
     * @return count result
     */
    @FFINameAlias("PartialCount")
    long partialCount(VID_T beg, VID_T end);

    /** Clear this vertex set. */
    @FFINameAlias("Clear")
    void clear();

    /**
     * Swap the state with another vertex set.
     *
     * @param rhs another vertex set
     */
    @FFINameAlias("Swap")
    void swap(@CXXReference DenseVertexSet<VID_T> rhs);

    /**
     * Get the underlying representation for this vertex set, a BitSet.
     *
     * @return the underlying bitset.
     * @see Bitset
     */
    @CXXReference
    Bitset GetBitset();

    /**
     * Check whether a empty set.
     *
     * @return empty or not
     */
    boolean Empty();

    /**
     * Check whether any vertices inserted in a certain range.
     *
     * @param beg begin index.
     * @param end end index.
     * @return empty or not
     */
    boolean PartialEmpty(VID_T beg, VID_T end);

    /**
     * Factory for DenseVertexSet
     *
     * @param <VID_T> vertex id inner representation type
     */
    @FFIFactory
    interface Factory<VID_T> {

        /**
         * Create a new instance of DenseVertexSet. Remember to call {@link
         * DenseVertexSet#init(VertexRange)} to init the obj.The actual storage is in C++ memory.
         *
         * @return a new instance.
         */
        DenseVertexSet<VID_T> create();
    }
}
