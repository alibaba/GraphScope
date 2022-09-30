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

package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.graphx.GraphXCSR;
import com.alibaba.graphscope.graphx.GraphXVertexMap;

public interface BaseGraphXFragment<OID_T, VID_T, VD_T, ED_T>
        extends EdgecutFragment<OID_T, VID_T, VD_T, ED_T> {

    long id();

    @FFINameAlias("GetIEBegin")
    PropertyNbrUnit<VID_T> getIEBegin(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetIEEnd")
    PropertyNbrUnit<VID_T> getIEEnd(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOEBegin")
    PropertyNbrUnit<VID_T> getOEBegin(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOEEnd")
    PropertyNbrUnit<VID_T> getOEEnd(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetCSR")
    @CXXReference
    GraphXCSR<VID_T> getCSR();

    @FFINameAlias("GetVM")
    @CXXReference
    GraphXVertexMap<OID_T, VID_T> getVM();

    @FFINameAlias("GetInEdgeNum")
    long getInEdgeNum();

    @FFINameAlias("GetOutEdgeNum")
    long getOutEdgeNum();
}
