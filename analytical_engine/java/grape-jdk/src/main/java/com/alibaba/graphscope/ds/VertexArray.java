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

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_VERTEX_ARRAY;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_VERTEX_ARRAY_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

/**
 * Vertex Array <a
 * href="https://github.com/alibaba/libgrape-lite/blob/master/grape/utils/vertex_array.h#L203">grape::VertexArray</a>.
 * An array which each slot binds to a vertex. Different from {@link GSVertexArray}, this class
 * VID_T as a template parameter rather than fixed to int64_t.
 *
 * @param <T> vertex data type.
 * @param <VID> vertex id type.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(GRAPE_VERTEX_ARRAY_H)
@FFITypeAlias(GRAPE_VERTEX_ARRAY)
public interface VertexArray<T, VID> extends FFIPointer, CXXPointer {

    /**
     * Init a vertex array with a range of vertices.
     *
     * @param range vertex range.
     */
    @FFINameAlias("Init")
    void init(@CXXReference VertexRange<VID> range);

    /**
     * Init a vertex array with a range of vertices, with initial value specified.
     *
     * @param range vertex range.
     * @param val initial value.
     */
    @FFINameAlias("Init")
    void init(@CXXReference VertexRange<VID> range, @CXXReference T val);

    /**
     * Full fill the vertex array with specified value.
     *
     * @param val value to fill in this array.
     */
    @FFINameAlias("SetValue")
    void setValue(@CXXReference T val);

    /**
     * Full fill the vertex array with specified value in a specific range.
     *
     * @param range value to fill in this array.
     * @param val vertex range.
     */
    @FFINameAlias("SetValue")
    void setValue(@CXXReference VertexRange<VID> range, @CXXReference T val);

    /**
     * Bind a specific vertex with one value.
     *
     * @param vertex vertex.
     * @param val value to bind.
     */
    @FFINameAlias("SetValue")
    void setValue(@CXXReference Vertex<VID> vertex, @CXXReference T val);

    /**
     * Get the data bound to a vertex.
     *
     * @param vertex querying vertex.
     * @return bound value.
     */
    @FFINameAlias("GetValue")
    @CXXOperator("[]")
    @CXXReference
    T get(@CXXReference Vertex<VID> vertex);

    /**
     * Get the underlying vertex range.
     *
     * @return the underlying vertex range.
     */
    @CXXReference
    VertexRange<VID> GetVertexRange();

    /**
     * Factory class for VertexArray.
     *
     * @param <T> vertex data type.
     * @param <VID> vertex id type.
     */
    @FFIFactory
    interface Factory<T, VID> {

        /**
         * Create an empty vertex Array. Please call {@link VertexArray#init(VertexRange)} to init
         * this array.
         *
         * @return created vertex array.
         */
        VertexArray<T, VID> create();
    }
}
