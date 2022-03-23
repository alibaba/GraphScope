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
