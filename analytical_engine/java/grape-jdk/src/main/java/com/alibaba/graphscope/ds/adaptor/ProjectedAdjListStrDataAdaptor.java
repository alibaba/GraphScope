package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.fastffi.impl.CXXStdString;
import com.alibaba.graphscope.ds.ProjectedAdjListStrData;

public class ProjectedAdjListStrDataAdaptor<VID_T> implements AdjList<VID_T, CXXStdString> {
    public static final String TYPE = "ProjectedAdjListStrData";
    private ProjectedAdjListStrData<VID_T> adjList;

    @Override
    public String type() {
        return TYPE;
    }

    public ProjectedAdjListStrDataAdaptor(ProjectedAdjListStrData<VID_T> adj) {
        adjList = adj;
    }

    public ProjectedAdjListStrData getProjectedAdjList() {
        return adjList;
    }

    @Override
    public Nbr<VID_T, CXXStdString> begin() {
        return new ProjectedNbrStrDataAdaptor<>(adjList.begin());
    }

    @Override
    public Nbr<VID_T, CXXStdString> end() {
        return new ProjectedNbrStrDataAdaptor<>(adjList.end());
    }

    @Override
    public long size() {
        return adjList.size();
    }
}
