package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.graphscope.ds.GrapeAdjList;

public class GrapeAdjListAdaptor<VID_T, EDATA_T> implements AdjList<VID_T, EDATA_T> {
    private GrapeAdjList adjList;

    @Override
    public String type() {
        return "GrapeAdjList";
    }

    public GrapeAdjListAdaptor(GrapeAdjList adj) {
        adjList = adj;
    }

    @Override
    public Nbr<VID_T, EDATA_T> begin() {
        return (Nbr<VID_T, EDATA_T>) adjList.begin();
    }

    @Override
    public Nbr<VID_T, EDATA_T> end() {
        return (Nbr<VID_T, EDATA_T>) adjList.end();
    }

    @Override
    public long size() {
        return adjList.size();
    }
}
