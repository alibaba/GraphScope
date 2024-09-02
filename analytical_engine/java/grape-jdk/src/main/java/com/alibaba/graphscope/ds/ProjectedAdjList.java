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

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.CXXPointer;

import java.util.Iterator;

/**
 * AdjList used by {@link com.alibaba.graphscope.fragment.ArrowProjectedFragment}, java wrapper for
 * <a
 * href="https://github.com/alibaba/GraphScope/blob/main/analytical_engine/core/fragment/arrow_projected_fragment.h#L260">ProjectedAdjList</a>.
 *
 * @param <VID_T> vertex id type.
 * @param <EDATA_T> edge data type.
 */
public interface ProjectedAdjList<VID_T, EDATA_T> {

    /**
     * Get the first Nbr.
     *
     * @return first Nbr.
     */
    ProjectedNbr<VID_T, EDATA_T> begin();

    /**
     * Get the last Nbr.
     *
     * @return last Nbr.
     */
    ProjectedNbr<VID_T, EDATA_T> end();

    /**
     * Size for this AdjList, i.e. number of nbrs.
     *
     * @return size.
     */
    long size();

    /**
     * Check empty.
     *
     * @return true if no nbr.
     */
    boolean empty();

    /**
     * Check no-empty.
     *
     * @return false if empty.
     */
    boolean notEmpty();
    

    public class ProjectedAdjListImpl<VID_T, EDATA_T> implements ProjectedAdjList<VID_T, EDATA_T> {
        private ProjectedNbr<VID_T, EDATA_T> begin;
        private ProjectedNbr<VID_T, EDATA_T> end;
        private int elementSize;

        public ProjectedAdjListImpl(ProjectedNbr<VID_T, EDATA_T> begin, ProjectedNbr<VID_T, EDATA_T> end) {
            this.begin = begin;
            this.end = end;
            //If VID_T is long, elementSize is 16, otherwise 8
            elementSize = 16;
        }

        ProjectedNbr<VID_T, EDATA_T> begin() {
            return begin;
        }

        ProjectedNbr<VID_T, EDATA_T> end() {
            return end;
        }

        long size() {
            return (end.getAddress() - begin.getAddress()) / elementSize;
        }

        boolean empty() {
            return begin.eq(end);
        }

        boolean notEmpty() {
            return !empty();
        }

        /**
         * The iterator for ProjectedAdjList. You can use enhanced for loop instead of directly using
         * this.
         *
         * @return the iterator.
         */
        default Iterable<ProjectedNbr<VID_T, EDATA_T>> iterable() {
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
}
