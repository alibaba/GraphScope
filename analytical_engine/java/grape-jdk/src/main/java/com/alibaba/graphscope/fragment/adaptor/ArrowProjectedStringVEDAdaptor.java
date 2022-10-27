package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.fastffi.impl.CXXStdString;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.ProjectedAdjListStrDataAdaptor;
import com.alibaba.graphscope.fragment.ArrowProjectedStringVEDFragment;
import com.alibaba.graphscope.fragment.FragmentType;

public class ArrowProjectedStringVEDAdaptor<OID_T, VID_T>
        extends AbstractArrowProjectedAdaptor<OID_T, VID_T, CXXStdString, CXXStdString> {

    private final ArrowProjectedStringVEDFragment<OID_T, VID_T> fragment;

    @Override
    public String toString() {
        return "ArrowProjectedStrVEDAdaptor{" + "fragment=" + fragment + '}';
    }

    @Override
    public FragmentType fragmentType() {
        return FragmentType.ArrowProjectedStrVEDFragment;
    }

    public ArrowProjectedStringVEDAdaptor(ArrowProjectedStringVEDFragment<OID_T, VID_T> frag) {
        super(frag);
        fragment = frag;
    }

    public ArrowProjectedStringVEDFragment<OID_T, VID_T> getArrowProjectedStrVEDFragment() {
        return fragment;
    }

    @Override
    public AdjList<VID_T, CXXStdString> getIncomingAdjList(Vertex<VID_T> vertex) {
        return new ProjectedAdjListStrDataAdaptor<>(fragment.getIncomingAdjList(vertex));
    }

    @Override
    public AdjList<VID_T, CXXStdString> getOutgoingAdjList(Vertex<VID_T> vertex) {
        return new ProjectedAdjListStrDataAdaptor<>(fragment.getOutgoingAdjList(vertex));
    }

    /**
     * Get the data on vertex.
     *
     * @param vertex querying vertex.
     * @return vertex data
     */
    @Override
    public CXXStdString getData(Vertex<VID_T> vertex) {
        return fragment.getData(vertex);
    }
}
