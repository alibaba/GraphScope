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

import com.alibaba.graphscope.ds.PrimitiveTypedArray;
import com.alibaba.graphscope.ds.ProjectedAdjList;
import com.alibaba.graphscope.ds.StringTypedArray;

public class ProjectedAdjListAdaptor<VID_T, EDATA_T> implements AdjList<VID_T, EDATA_T> {
    public static final String TYPE = "ProjectedAdjList";
    private ProjectedAdjList<VID_T, EDATA_T> adjList;
    private PrimitiveTypedArray<EDATA_T> primitiveTypedArray;
    private StringTypedArray stringTypedArray;

    @Override
    public String type() {
        return TYPE;
    }

    public ProjectedAdjListAdaptor(
            ProjectedAdjList<VID_T, EDATA_T> adj,
            PrimitiveTypedArray<EDATA_T> primitiveTypedArray) {
        adjList = adj;
        this.primitiveTypedArray = primitiveTypedArray;
    }

    public ProjectedAdjListAdaptor(
            ProjectedAdjList<VID_T, EDATA_T> adj, StringTypedArray stringTypedArray) {
        adjList = adj;
        this.stringTypedArray = stringTypedArray;
    }

    public ProjectedAdjList getProjectedAdjList() {
        return adjList;
    }

    public EDATA_T getEdata(long index) {
        if (primitiveTypedArray != null) {
            return primitiveTypedArray.get(index);
        }
        return (EDATA_T) stringTypedArray.get(index);
    }

    @Override
    public Nbr<VID_T, EDATA_T> begin() {
        return new ProjectedNbrAdaptor<>(adjList.begin(), this);
    }

    @Override
    public Nbr<VID_T, EDATA_T> end() {
        return new ProjectedNbrAdaptor<>(adjList.end(), this);
    }

    @Override
    public long size() {
        return adjList.size();
    }
}
