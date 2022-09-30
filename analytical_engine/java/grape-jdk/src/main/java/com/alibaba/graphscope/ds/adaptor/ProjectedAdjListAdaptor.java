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

import com.alibaba.graphscope.ds.ProjectedAdjList;

public class ProjectedAdjListAdaptor<VID_T, EDATA_T> implements AdjList<VID_T, EDATA_T> {
    public static final String TYPE = "ProjectedAdjList";
    private ProjectedAdjList<VID_T, EDATA_T> adjList;

    @Override
    public String type() {
        return TYPE;
    }

    public ProjectedAdjListAdaptor(ProjectedAdjList<VID_T, EDATA_T> adj) {
        adjList = adj;
    }

    public ProjectedAdjList getProjectedAdjList() {
        return adjList;
    }

    @Override
    public Nbr<VID_T, EDATA_T> begin() {
        return new ProjectedNbrAdaptor<>(adjList.begin());
    }

    @Override
    public Nbr<VID_T, EDATA_T> end() {
        return new ProjectedNbrAdaptor<>(adjList.end());
    }

    @Override
    public long size() {
        return adjList.size();
    }
}
