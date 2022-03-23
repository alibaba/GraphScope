package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.graphscope.ds.DestList;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;

public interface BaseArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>
        extends EdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> {

    long id();

    @FFINameAlias("get_out_edges_ptr")
    PropertyNbrUnit<VID_T> getOutEdgesPtr();

    @FFINameAlias("get_in_edges_ptr")
    PropertyNbrUnit<VID_T> getInEdgesPtr();

    @FFINameAlias("get_oe_offsets_begin_ptr")
    long getOEOffsetsBeginPtr();

    @FFINameAlias("get_ie_offsets_begin_ptr")
    long getIEOffsetsBeginPtr();

    @FFINameAlias("get_oe_offsets_end_ptr")
    long getOEOffsetsEndPtr();

    @FFINameAlias("get_ie_offsets_end_ptr")
    long getIEOffsetsEndPtr();

    @FFINameAlias("GetInEdgeNum")
    long getInEdgeNum();

    @FFINameAlias("GetOutEdgeNum")
    long getOutEdgeNum();

    @FFINameAlias("GetIncomingEdgeNum")
    long getIncomingEdgeNum();

    @FFINameAlias("GetOutgoingEdgeNum")
    long getOutgoingEdgeNum();

    @FFINameAlias("get_arrow_fragment")
    @CXXValue
    StdSharedPtr<ArrowFragment<OID_T>> getArrowFragment();

    @FFINameAlias("vertex_label")
    int vertexLabel();

    @FFINameAlias("edge_label")
    int edgeLabel();

    @FFINameAlias("vertex_prop_id")
    int vertexPropId();

    @FFINameAlias("edge_prop_id")
    int edgePropId();

    @FFINameAlias("OEDests")
    @CXXValue
    DestList oeDestList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("IEDests")
    @CXXValue
    DestList ieDestList(@CXXReference Vertex<VID_T> vertex);
}
