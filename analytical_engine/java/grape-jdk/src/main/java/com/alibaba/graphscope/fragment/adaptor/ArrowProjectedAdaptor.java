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

package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.ProjectedAdjListAdaptor;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.FragmentType;

public class ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
        extends AbstractArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> {

    private ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment;

    @Override
    public String toString() {
        return "ArrowProjectedAdaptor{" + "fragment=" + fragment + '}';
    }

    public ArrowProjectedAdaptor(ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag) {
        super(frag);
        fragment = frag;
    }

    @Override
    public FragmentType fragmentType() {
        return FragmentType.ArrowProjectedFragment;
    }

    public ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> getArrowProjectedFragment() {
        return fragment;
    }

    @Override
    public AdjList<VID_T, EDATA_T> getIncomingAdjList(Vertex<VID_T> vertex) {
        return new ProjectedAdjListAdaptor<>(fragment.getIncomingAdjList(vertex));
    }

    @Override
    public AdjList<VID_T, EDATA_T> getOutgoingAdjList(Vertex<VID_T> vertex) {
        return new ProjectedAdjListAdaptor<>(fragment.getOutgoingAdjList(vertex));
    }

    /**
     * Get the data on vertex.
     *
     * @param vertex querying vertex.
     * @return vertex data
     */
    @Override
    public VDATA_T getData(Vertex<VID_T> vertex) {
        return fragment.getData(vertex);
    }
}
