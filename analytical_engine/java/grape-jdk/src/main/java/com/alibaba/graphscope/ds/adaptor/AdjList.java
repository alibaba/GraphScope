/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.graphscope.ds.NbrBase;

import java.util.Iterator;
import java.util.Objects;

/**
 * This interface define the neighboring vertices and edge data for a vertex.
 *
 * @param <VID_T> vertex id type.
 * @param <EDATA_T> edge data type.
 */
public interface AdjList<VID_T, EDATA_T> {

    String type();

    /**
     * Get the begin Nbr.
     *
     * @return the first Nbr.
     */
    Nbr<VID_T, EDATA_T> begin();

    /**
     * Get the last Nbr.
     *
     * @return the last Nbr.
     */
    Nbr<VID_T, EDATA_T> end();

    /**
     * Get the size of this adjList.
     *
     * @return size
     */
    long size();

    /**
     * The iterator for adjlist. You can use enhanced for loop instead of directly using this.
     *
     * @return the iterator.
     */
    default Iterable<Nbr<VID_T, EDATA_T>> iterable() {
        if (type().equals(GrapeAdjListAdaptor.TYPE)) {
            return () ->
                    new Iterator<Nbr<VID_T, EDATA_T>>() {
                        GrapeNbrAdaptor<VID_T, EDATA_T> beginPtr =
                                (GrapeNbrAdaptor<VID_T, EDATA_T>) begin();
                        GrapeNbrAdaptor<VID_T, EDATA_T> endPtr =
                                (GrapeNbrAdaptor<VID_T, EDATA_T>) end();
                        GrapeNbrAdaptor<VID_T, EDATA_T> curPtr =
                                (GrapeNbrAdaptor<VID_T, EDATA_T>) begin();
                        long currentAddress = 0;
                        long endAddress = 0;
                        long elementSize = 0;

                        // There are cases where begin() and end() nbrs are null.
                        {
                            if (Objects.nonNull(beginPtr.getGrapeNbr())
                                    && beginPtr.getAddress() > 0) {
                                this.currentAddress = beginPtr.getAddress();
                                this.elementSize = beginPtr.getGrapeNbr().elementSize();
                            }
                            if (Objects.nonNull(endPtr.getGrapeNbr()) && endPtr.getAddress() > 0) {
                                this.endAddress = endPtr.getAddress();
                            }
                        }

                        public boolean hasNext() {
                            return this.currentAddress != this.endAddress;
                        }

                        public Nbr<VID_T, EDATA_T> next() {
                            curPtr.setAddress(this.currentAddress);
                            this.currentAddress += this.elementSize;
                            return curPtr;
                        }
                    };
        } else if (type().equals(ProjectedAdjListAdaptor.TYPE)) {
            return () ->
                    new Iterator<Nbr<VID_T, EDATA_T>>() {
                        ProjectedNbrAdaptor<VID_T, EDATA_T> end =
                                (ProjectedNbrAdaptor<VID_T, EDATA_T>) end();
                        Nbr<VID_T, EDATA_T> curPtr = begin().dec();
                        boolean flag = false;

                        @Override
                        public boolean hasNext() {
                            if (!flag) {
                                curPtr = curPtr.inc();
                                flag = !curPtr.eq(end);
                            }
                            return flag;
                        }
                        //
                        @Override
                        public Nbr<VID_T, EDATA_T> next() {
                            flag = false;
                            return curPtr;
                        }
                    };
        }
        return null;
    }

    default Iterable<? extends NbrBase<VID_T, EDATA_T>> nbrBases() {
        if (this
                instanceof GrapeAdjListAdaptor) { // use string equal cause performance degradation.
            return ((GrapeAdjListAdaptor<VID_T, EDATA_T>) this).getAdjList().locals();
        } else if (this instanceof ProjectedAdjListAdaptor) {
            return ((ProjectedAdjListAdaptor<VID_T, EDATA_T>) this)
                    .getProjectedAdjList()
                    .iterable();
        } else {
            throw new IllegalStateException("not supported");
        }
    }
}
