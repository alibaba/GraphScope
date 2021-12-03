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

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_VERTEX_RANGE;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_VERTEX_ARRAY_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.CXXValueRange;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import java.util.function.Consumer;

/**
 * Vertex Range is an abstraction for a range of vertices. Corresponding C++ <a
 * href="https://github.com/alibaba/libgrape-lite/blob/master/grape/utils/vertex_array.h#L134">grape::VertexRange</a>
 *
 * @param <VID_T> vertex id type.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(GRAPE_VERTEX_ARRAY_H)
@FFITypeAlias(GRAPE_VERTEX_RANGE)
public interface VertexRange<VID_T> extends FFIPointer, CXXPointer, CXXValueRange<Vertex<VID_T>> {
    /**
     * Return the Begin vertex for this VertexRange. Note that invoking this methods multiple times
     * will return the same reference, java object is not created when this method is called.
     *
     * @return Vertex&lt;VID_T&lt; the first vertex
     */
    @CXXReference
    Vertex<VID_T> begin();

    /**
     * Return the last vertex for this VertexRange. Note that invoking this methods multiple times
     * will return the same reference, java object is not created when this method is called.
     *
     * @return Vertex&lt;VID_T&lt; the last vertex
     */
    @CXXReference
    Vertex<VID_T> end();

    /**
     * Return the number of vertices in this vertex range.
     *
     * @return the size.
     */
    long size();

    /**
     * Update the left bound and right bound.
     *
     * @param begin left(begin) VID_T.
     * @param end right(end) VID_T.
     */
    void SetRange(@CXXValue VID_T begin, @CXXValue VID_T end);

    /**
     * Takes one consumer function as input, apply to each vertex.
     *
     * @param action consumer.
     */
    default void forEachPlus(Consumer<Vertex<VID_T>> action) {
        Vertex<VID_T> vertex = begin();
        VID_T endValue = end().GetValue();
        while (!vertex.GetValue().equals(endValue)) {
            action.accept(vertex);
            vertex.inc();
        }
    }

    /**
     * Factory type for vertex range.
     *
     * @param <VID_T> vertex id type.
     */
    @FFIFactory
    interface Factory<VID_T> {

        /**
         * Creating a VertexRange instance.Please call {@link VertexRange#SetRange(Object, Object)}
         * for initialization.
         *
         * @return created obj.
         */
        VertexRange<VID_T> create();

        /**
         * Creating a VertexRange with initial range.
         *
         * @param begin left bound.
         * @param end right bound.
         * @return created obj.
         */
        VertexRange<VID_T> create(@CXXValue VID_T begin, @CXXValue VID_T end);
    }
}
