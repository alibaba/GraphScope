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

import static com.alibaba.graphscope.utils.CppClassName.PROJECTED_NBR;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

/**
 * Definition of Nbr for projected fragment <a
 * href="https://github.com/alibaba/GraphScope/blob/main/analytical_engine/core/fragment/arrow_projected_fragment.h#L121">ProjectedNbr</a>.
 * Representing the neighboring vertex.
 *
 * @param <VID_T> vertex id type.
 * @param <EDATA_T> edge data type.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(PROJECTED_NBR)
public interface ProjectedNbr<VID_T, EDATA_T> extends FFIPointer {

    /**
     * Get the neighbor vertex.
     *
     * @return vertex.
     */
    @CXXValue
    Vertex<VID_T> neighbor();

    /**
     * Edge id for this Nbr, inherited from property graph.
     *
     * @return edge id.
     */
    @FFINameAlias("edge_id")
    long edgeId();

    /**
     * Get the edge data.
     *
     * @return edge data.
     */
    EDATA_T data();

    /**
     * Self increment.
     *
     * @return increated pointer.
     */
    @CXXOperator("++")
    @CXXReference
    ProjectedNbr<VID_T, EDATA_T> inc();

    /**
     * Check equality.
     *
     * @param rhs Nbr to be compared with
     * @return true if is the same pointer.
     */
    @CXXOperator("==")
    boolean eq(@CXXReference ProjectedNbr<VID_T, EDATA_T> rhs);

    /**
     * Self decrement.
     *
     * @return decreased pointer
     */
    @CXXOperator("--")
    @CXXReference
    ProjectedNbr<VID_T, EDATA_T> dec();
}
