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

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_VERTEX;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_VERTEX_RANGE;
import static com.alibaba.graphscope.utils.CppClassName.GS_VERTEX_ARRAY;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_VERTEX_ARRAY_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GS_CORE_CONFIG_H;
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
 * A java wrapper for gs template class <a href=
 * "https://github.com/alibaba/libgrape-lite/blob/master/grape/utils/vertex_array.h#L203">grape::VertexArray</a>.
 *
 * <p>Here the VID_T is set to long(int64_t).
 *
 * @param <T> vertex data type.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(GS_CORE_CONFIG_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(GRAPE_VERTEX_ARRAY_H)
@CXXHead(system = "cstdint")
@FFITypeAlias(GS_VERTEX_ARRAY)
public interface GSVertexArray<T> extends FFIPointer, CXXPointer {

    /**
     * Init a vertex Array with a range of vertices.
     *
     * @param range vertex range.
     */
    @FFINameAlias("Init")
    void init(
            @CXXReference @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>") VertexRange<Long> range);

    /**
     * Init a vertex Array with a range of vertices, with default value.
     *
     * @param range vertex range.
     * @param val default value.
     */
    @FFINameAlias("Init")
    void init(
            @CXXReference @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>") VertexRange<Long> range,
            @CXXReference T val);

    /**
     * Full fill the vertex array with the specified value.
     *
     * @param val vertex data.
     */
    @FFINameAlias("SetValue")
    void setValue(@CXXReference T val);

    /**
     * Set the data for a range of vertices.
     *
     * @param range vertex range.
     * @param val value to set.
     */
    @FFINameAlias("SetValue")
    void setValue(
            @CXXReference @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>") VertexRange<Long> range,
            @CXXReference T val);

    /**
     * Set the data for a specific vertex.
     *
     * @param vertex vertex.
     * @param val vertex data.
     */
    @FFINameAlias("SetValue")
    void setValue(
            @CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> vertex,
            @CXXReference T val);

    /**
     * Get the data bound to the querying vertex.
     *
     * @param vertex querying vertex.
     * @return vertex data.
     */
    @FFINameAlias("GetValue")
    @CXXOperator("[]")
    @CXXReference
    T get(@CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> vertex);

    /**
     * Get the vertex range for this vertex array.
     *
     * @return the range of vertices.
     */
    @CXXReference
    @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>")
    VertexRange<Long> GetVertexRange();

    @FFINameAlias("Swap")
    void swap(@CXXReference GSVertexArray<T> vertexArray);

    /**
     * Factory GSVertexArray.
     *
     * @param <T> vertex data type.
     */
    @FFIFactory
    interface Factory<T> {
        GSVertexArray<T> create();
    }
}
