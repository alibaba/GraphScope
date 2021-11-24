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
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_VERTEX_ARRAY_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.CXXValueRangeElement;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

/**
 * Java Wrapper for <a href=
 * "https://github.com/alibaba/libgrape-lite/blob/master/grape/utils/vertex_array.h#L40">grape::Vertex</a>.
 *
 * @param <VID_T> vertex id type. Long recommended.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(GRAPE_VERTEX_ARRAY_H)
@FFITypeAlias(GRAPE_VERTEX)
@CXXTemplate(
        cxx = {"uint64_t"},
        java = {"Long"})
public interface Vertex<VID_T> extends FFIPointer, CXXPointer, CXXValueRangeElement<Vertex<VID_T>> {

    /**
     * Return a <em>deep</em> copy of current vertex.
     *
     * @return the copied vertex.
     */
    @CXXOperator("*&")
    @CXXValue
    Vertex<VID_T> copy();

    /**
     * Note this is not necessary to be a prefix increment
     *
     * @return current vertex with vertex.id + 1
     */
    @CXXOperator("++")
    @CXXReference
    Vertex<VID_T> inc();

    /**
     * Judge whether Two vertex id are the same.
     *
     * @param vertex vertex to compare with.
     * @return equal or not.
     */
    @CXXOperator("==")
    boolean eq(@CXXReference Vertex<VID_T> vertex);

    /**
     * Get vertex id.
     *
     * @return vertex id.
     */
    VID_T GetValue();

    /**
     * Set vertex id.
     *
     * @param id id to be set.
     */
    void SetValue(VID_T id);

    /**
     * Factory class to create vertex instance.
     *
     * @param <VID_T> vertex id type.
     */
    @FFIFactory
    interface Factory<VID_T> {
        Vertex<VID_T> create();
    }
}
