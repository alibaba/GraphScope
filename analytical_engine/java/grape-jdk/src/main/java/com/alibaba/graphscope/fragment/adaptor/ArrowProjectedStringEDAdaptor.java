package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.fastffi.impl.CXXStdString;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.ProjectedAdjListStrDataAdaptor;
import com.alibaba.graphscope.fragment.ArrowProjectedStringEDFragment;
import com.alibaba.graphscope.fragment.FragmentType;

public class ArrowProjectedStringEDAdaptor<OID_T, VID_T, VDATA_T>
        extends AbstractArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, CXXStdString> {

    private ArrowProjectedStringEDFragment<OID_T, VID_T, VDATA_T> fragment;

    @Override
    public String toString() {
        return "ArrowProjectedStrEDAdaptor{" + "fragment=" + fragment + '}';
    }

    @Override
    public FragmentType fragmentType() {
        return FragmentType.ArrowProjectedStrEDFragment;
    }

    public ArrowProjectedStringEDAdaptor(
            ArrowProjectedStringEDFragment<OID_T, VID_T, VDATA_T> frag) {
        super(frag);
        fragment = frag;
    }

    public ArrowProjectedStringEDFragment<OID_T, VID_T, VDATA_T> getArrowProjectedStrEDFragment() {
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
    public VDATA_T getData(Vertex<VID_T> vertex) {
        return fragment.getData(vertex);
    }
}
