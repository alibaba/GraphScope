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

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_ADJ_LIST;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_ADJ_LIST_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_TYPES_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXPointerRange;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

/**
 * AdjList is the data structure representing the edges(destination vertex and edge data) of a
 * single vertex. The edges are representing in form of {@link GrapeNbr}.
 *
 * <p>This is a wrapper for <a
 * href="https://github.com/alibaba/libgrape-lite/blob/master/grape/graph/adj_list.h">C++ AdjList
 * class</a>
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead({GRAPE_ADJ_LIST_H, GRAPE_TYPES_H})
@FFITypeAlias(GRAPE_ADJ_LIST)
public interface GrapeAdjList<VID_T, EDATA_T>
        extends FFIPointer, CXXPointer, CXXPointerRange<GrapeNbr<VID_T, EDATA_T>> {
    /**
     * Get the begin Nbr.
     *
     * @return the first Nbr.
     */
    default GrapeNbr<VID_T, EDATA_T> begin() {
        return begin_pointer();
    }
    /**
     * Get the last Nbr.
     *
     * @return the last Nbr.
     */
    default GrapeNbr<VID_T, EDATA_T> end() {
        return end_pointer();
    }

    /**
     * Get the begin Nbr.
     *
     * @return the first Nbr.
     */
    GrapeNbr<VID_T, EDATA_T> begin_pointer();

    /**
     * Get the last Nbr.
     *
     * @return the last Nbr.
     */
    GrapeNbr<VID_T, EDATA_T> end_pointer();

    /**
     * Get the size of this adjList.
     *
     * @return size
     */
    @FFINameAlias("Size")
    long size();
}
