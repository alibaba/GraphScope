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

import com.alibaba.graphscope.ds.PrimitiveTypedArray;
import com.alibaba.graphscope.ds.StringTypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.ProjectedAdjListAdaptor;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.TypeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>
        extends AbstractArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> {
    private Logger logger = LoggerFactory.getLogger(ArrowProjectedAdaptor.class.getName());

    private ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment;
    private PrimitiveTypedArray<VDATA_T> primitiveVDataArray;
    private PrimitiveTypedArray<EDATA_T> primitiveEDataArray;
    private StringTypedArray complexVDataArray;
    private StringTypedArray complexEDataArray;
    private boolean vdataPrimitive, edataPrimitive;

    @Override
    public String toString() {
        return "ArrowProjectedAdaptor{" + "fragment=" + fragment + '}';
    }

    public ArrowProjectedAdaptor(
            ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
            Class<? extends OID_T> oidClass,
            Class<? extends VID_T> vidClass,
            Class<? extends VDATA_T> vdataClass,
            Class<? extends EDATA_T> edataClass) {
        super(frag, oidClass, vidClass, vdataClass, edataClass);
        fragment = frag;
        if (TypeUtils.isPrimitive(vdataClass)) {
            primitiveVDataArray = FFITypeFactoryhelper.newPrimitiveTypedArray(vdataClass);
            primitiveVDataArray.setAddress(frag.getVdataArrayAccessor().getAddress());
            vdataPrimitive = true;
        } else {
            complexVDataArray = FFITypeFactoryhelper.newStringTypedArray();
            complexVDataArray.setAddress(frag.getVdataArrayAccessor().getAddress());
            vdataPrimitive = false;
        }
        if (TypeUtils.isPrimitive(edataClass)) {
            primitiveEDataArray = FFITypeFactoryhelper.newPrimitiveTypedArray(edataClass);
            primitiveEDataArray.setAddress(frag.getEdataArrayAccessor().getAddress());
            edataPrimitive = true;
        } else {
            complexEDataArray = FFITypeFactoryhelper.newStringTypedArray();
            complexEDataArray.setAddress(frag.getEdataArrayAccessor().getAddress());
            edataPrimitive = false;
        }
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
        if (edataPrimitive)
            return new ProjectedAdjListAdaptor<>(
                    fragment.getIncomingAdjList(vertex), primitiveEDataArray);
        else
            return new ProjectedAdjListAdaptor<>(
                    fragment.getIncomingAdjList(vertex), complexEDataArray);
    }

    @Override
    public AdjList<VID_T, EDATA_T> getOutgoingAdjList(Vertex<VID_T> vertex) {
        if (edataPrimitive)
            return new ProjectedAdjListAdaptor<>(
                    fragment.getOutgoingAdjList(vertex), primitiveEDataArray);
        else
            return new ProjectedAdjListAdaptor<>(
                    fragment.getOutgoingAdjList(vertex), complexEDataArray);
    }

    /**
     * Get the data on vertex.
     *
     * @param vertex querying vertex.
     * @return vertex data
     */
    @Override
    public VDATA_T getData(Vertex<VID_T> vertex) {
        if (vdataPrimitive) {
            if (primitiveVDataArray == null) {
                throw new IllegalStateException(
                        "primitive vdata array is null, " + getVdataClass().getName());
            }
            return primitiveVDataArray.get((Long) vertex.getValue());
        } else {
            return (VDATA_T) complexVDataArray.get((Long) vertex.getValue());
        }
    }
}
