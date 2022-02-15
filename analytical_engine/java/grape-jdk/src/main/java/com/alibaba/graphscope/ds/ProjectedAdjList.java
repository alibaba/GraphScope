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

import static com.alibaba.graphscope.utils.CppClassName.PROJECTED_ADJ_LIST;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import java.util.Iterator;

/**
 * AdjList used by {@link com.alibaba.graphscope.fragment.ArrowProjectedFragment}, java wrapper for
 * <a
 * href="https://github.com/alibaba/GraphScope/blob/main/analytical_engine/core/fragment/arrow_projected_fragment.h#L260">ProjectedAdjList</a>.
 *
 * @param <VID_T> vertex id type.
 * @param <EDATA_T> edge data type.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(PROJECTED_ADJ_LIST)
public interface ProjectedAdjList<VID_T, EDATA_T> extends FFIPointer {

    /**
     * Get the the first Nbr.
     *
     * @return first Nbr.
     */
    @CXXValue
    ProjectedNbr<VID_T, EDATA_T> begin();

    /**
     * Get the last Nbr.
     *
     * @return last Nbr.
     */
    @CXXValue
    ProjectedNbr<VID_T, EDATA_T> end();

    /**
     * Size for this AdjList, i.e. number of nbrs.
     *
     * @return size.
     */
    @FFINameAlias("Size")
    long size();

    /**
     * Check empty.
     *
     * @return true if no nbr.
     */
    @FFINameAlias("Empty")
    boolean empty();

    /**
     * Check no-empty.
     *
     * @return false if empty.
     */
    @FFINameAlias("NotEmpty")
    boolean notEmpty();

    /**
     * The iterator for ProjectedAdjList. You can use enhanced for loop instead of directly using
     * this.
     *
     * @return the iterator.
     */
    default Iterable<ProjectedNbr<VID_T, EDATA_T>> iterator() {
        return () ->
                new Iterator<ProjectedNbr<VID_T, EDATA_T>>() {
                    ProjectedNbr<VID_T, EDATA_T> cur = begin().dec();
                    ProjectedNbr<VID_T, EDATA_T> end = end();
                    boolean flag = false;

                    @Override
                    public boolean hasNext() {
                        if (!flag) {
                            cur = cur.inc();
                            flag = !cur.eq(end);
                        }
                        return flag;
                    }

                    @Override
                    public ProjectedNbr<VID_T, EDATA_T> next() {
                        flag = false;
                        return cur;
                    }
                };
    }
}
