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

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_NBR;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_ADJ_LIST_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXPointerRangeElement;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

/**
 * Java wrpper for <a
 * href="https://github.com/alibaba/libgrape-lite/blob/master/grape/graph/adj_list.h#L35">grape::Nbr</a>,
 * representing an edge with dst vertex and edge data.
 *
 * @param <VID> vertex id type.
 * @param <EDATA> edge data type.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(GRAPE_ADJ_LIST_H)
@FFITypeAlias(GRAPE_NBR)
public interface Nbr<VID, EDATA> extends FFIPointer, CXXPointerRangeElement<Nbr<VID, EDATA>> {

    /**
     * Deep copy for current Nbr object.
     *
     * @return the copied object.
     */
    @CXXOperator("*&")
    @CXXValue
    Nbr<VID, EDATA> copy();

    /**
     * Get the neighboring vertex.
     *
     * @return vertex.
     */
    @FFIGetter
    @CXXReference
    Vertex<VID> neighbor();

    /**
     * Get the edge data.
     *
     * @return edge data.
     */
    @FFIGetter
    @CXXReference
    EDATA data();

    /**
     * Factory class for Nbr.
     *
     * @param <VID> vertex id type.
     * @param <EDATA> edge data type.
     */
    @FFIFactory
    interface Factory<VID, EDATA> {

        /**
         * Create a default Nbr.
         *
         * @return Nbr instance.
         */
        Nbr<VID, EDATA> create();

        /**
         * Create a Nbr with adjacent vertex.
         *
         * @param lid vertex id.
         * @return Nbr instance.
         */
        Nbr<VID, EDATA> create(VID lid);

        /**
         * Create a Nbr with adjacent vertex and edge data.
         *
         * @param lid vertex id.
         * @param edata edge data.
         * @return Nbr instance.
         */
        Nbr<VID, EDATA> create(VID lid, @CXXReference EDATA edata);
    }
}
