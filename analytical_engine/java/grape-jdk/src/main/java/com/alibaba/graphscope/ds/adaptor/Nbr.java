package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.graphscope.ds.Vertex;

public interface Nbr<VID_T, EDATA_T> {
    String type();

    /**
     * Get the neighboring vertex.
     *
     * @return vertex.
     */
    Vertex<VID_T> neighbor();

    /**
     * Get the edge data.
     *
     * @return edge data.
     */
    EDATA_T data();

    Nbr<VID_T, EDATA_T> inc();

    boolean eq(Nbr<VID_T, EDATA_T> rhs);

    Nbr<VID_T, EDATA_T> dec();
}
