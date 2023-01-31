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

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import java.util.Iterator;

/**
 * Vertex Range is an abstraction for a range of vertices. Corresponding C++ <a
 * href="https://github.com/alibaba/libgrape-lite/blob/master/grape/utils/vertex_array.h#L134">grape::VertexRange</a>
 *
 * @param <VID_T> vertex id type.
 */
@FFIGen
@CXXHead(GRAPE_VERTEX_ARRAY_H)
@FFITypeAlias(GRAPE_VERTEX_RANGE)
public interface VertexRange<VID_T> extends FFIPointer, CXXPointer {
    /**
     * Return the Begin vertex id for this VertexRange.
     *
     * @return  the first vertex id
     */
    @FFINameAlias("begin_value")
    @CXXValue
    VID_T beginValue();

    /**
     * Return the last vertex for this VertexRange.
     *
     * @return the last vertex id
     */
    @FFINameAlias("end_value")
    @CXXValue
    VID_T endValue();

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
     * Get iterator for vid=long
     * @return
     */
    default Iterable<Vertex<VID_T>> longIterable() {
        return () ->
                new Iterator<Vertex<VID_T>>() {
                    Vertex<Long> vertex = (Vertex<Long>) FFITypeFactoryhelper.newVertexLong();
                    Long curValue;
                    Long endValue;

                    {
                        vertex.setValue((Long) beginValue());
                        curValue = (Long) beginValue();
                        endValue = (Long) endValue();
                    }

                    public boolean hasNext() {
                        return !curValue.equals(endValue);
                    }

                    public Vertex<VID_T> next() {
                        vertex.setValue(curValue);
                        curValue += 1;
                        return (Vertex<VID_T>) vertex;
                    }
                };
    }

    default Iterable<Vertex<VID_T>> intIterable() {
        return () ->
                new Iterator<Vertex<VID_T>>() {
                    Vertex<Integer> vertex = (Vertex<Integer>) FFITypeFactoryhelper.newVertexInt();
                    Integer curValue;
                    Integer endValue;

                    {
                        vertex.setValue((Integer) beginValue());
                        curValue = (Integer) beginValue();
                        endValue = (Integer) endValue();
                    }

                    public boolean hasNext() {
                        return !curValue.equals(endValue);
                    }

                    public Vertex<VID_T> next() {
                        vertex.setValue(curValue);
                        curValue += 1;
                        return (Vertex<VID_T>) vertex;
                    }
                };
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
