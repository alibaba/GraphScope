package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.graphscope.ds.GrapeNbr;
import com.alibaba.graphscope.ds.Vertex;

public class GrapeNbrAdaptor<VID_T, EDATA_T> implements Nbr<VID_T, EDATA_T> {
    private GrapeNbr nbr;

    @Override
    public String type() {
        return "GrapeNbr";
    }

    public GrapeNbrAdaptor(GrapeNbr n) {
        nbr = n;
    }

    @Override
    public Vertex<VID_T> neighbor() {
        return nbr.neighbor();
    }

    @Override
    public EDATA_T data() {
        return (EDATA_T) nbr.data();
    }

    @Override
    public Nbr<VID_T, EDATA_T> inc() {
        return (Nbr<VID_T, EDATA_T>) nbr.inc();
    }

    @Override
    public boolean eq(Nbr<VID_T, EDATA_T> rhs) {
        return nbr.eq((GrapeNbr) rhs);
    }

    @Override
    public Nbr<VID_T, EDATA_T> dec() {
        return (Nbr<VID_T, EDATA_T>) nbr.dec();
    }
}
