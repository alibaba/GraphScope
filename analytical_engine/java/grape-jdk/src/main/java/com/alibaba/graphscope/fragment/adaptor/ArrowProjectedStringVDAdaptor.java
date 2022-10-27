package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.fastffi.impl.CXXStdString;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.ProjectedAdjListAdaptor;
import com.alibaba.graphscope.fragment.ArrowProjectedStringVDFragment;
import com.alibaba.graphscope.fragment.FragmentType;

public class ArrowProjectedStringVDAdaptor<OID_T, VID_T, EDATA_T>
        extends AbstractArrowProjectedAdaptor<OID_T, VID_T, CXXStdString, EDATA_T> {

    private ArrowProjectedStringVDFragment<OID_T, VID_T, EDATA_T> fragment;

    @Override
    public String toString() {
        return "ArrowProjectedStrVDAdaptor{" + "fragment=" + fragment + '}';
    }

    @Override
    public FragmentType fragmentType() {
        return FragmentType.ArrowProjectedStrVDFragment;
    }

    public ArrowProjectedStringVDAdaptor(
            ArrowProjectedStringVDFragment<OID_T, VID_T, EDATA_T> frag) {
        super(frag);
        fragment = frag;
    }

    public ArrowProjectedStringVDFragment<OID_T, VID_T, EDATA_T> getArrowProjectedStrVDFragment() {
        return fragment;
    }

    @Override
    public AdjList<VID_T, EDATA_T> getIncomingAdjList(Vertex<VID_T> vertex) {
        return new ProjectedAdjListAdaptor<>(fragment.getIncomingAdjList(vertex));
    }

    @Override
    public AdjList<VID_T, EDATA_T> getOutgoingAdjList(Vertex<VID_T> vertex) {
        return new ProjectedAdjListAdaptor<>(fragment.getOutgoingAdjList(vertex));
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
