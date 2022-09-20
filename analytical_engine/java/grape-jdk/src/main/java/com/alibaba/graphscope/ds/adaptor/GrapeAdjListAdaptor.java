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

import com.alibaba.graphscope.ds.GrapeAdjList;

/**
 * Wrap a grape nbr into one object that fit Nbr interface.
 *
 * @param <VID_T> vertex id type.
 * @param <EDATA_T> edge data type.
 */
public class GrapeAdjListAdaptor<VID_T, EDATA_T> implements AdjList<VID_T, EDATA_T> {
    public static final String TYPE = "GrapeAdjList";
    private GrapeAdjList<VID_T, EDATA_T> adjList;

    public GrapeAdjList<VID_T, EDATA_T> getAdjList() {
        return adjList;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public GrapeAdjListAdaptor(GrapeAdjList<VID_T, EDATA_T> adj) {
        adjList = adj;
    }

    @Override
    public Nbr<VID_T, EDATA_T> begin() {
        return new GrapeNbrAdaptor<>(adjList.begin());
    }

    @Override
    public Nbr<VID_T, EDATA_T> end() {
        return new GrapeNbrAdaptor<>(adjList.end());
    }

    @Override
    public long size() {
        return adjList.size();
    }
}
