package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.graphscope.ds.ProjectedNbr;
import com.alibaba.graphscope.ds.Vertex;

public class ProjectedNbrAdaptor<VID_T, EDATA_T> implements Nbr<VID_T, EDATA_T> {
    public static final String TYPE = "GrapeNbr";
    private ProjectedNbr<VID_T, EDATA_T> nbr;

    @Override
    public String type() {
        return TYPE;
    }

    public ProjectedNbrAdaptor(ProjectedNbr<VID_T, EDATA_T> n) {
        nbr = n;
    }

    @Override
    public Vertex<VID_T> neighbor() {
        return nbr.neighbor();
    }

    @Override
    public EDATA_T data() {
        return nbr.data();
    }

    @Override
    public Nbr<VID_T, EDATA_T> inc() {
        return (Nbr<VID_T, EDATA_T>) nbr.inc();
    }

    @Override
    public boolean eq(Nbr<VID_T, EDATA_T> rhs) {
        return nbr.eq((ProjectedNbr<VID_T, EDATA_T>) rhs);
    }

    @Override
    public Nbr<VID_T, EDATA_T> dec() {
        return (Nbr<VID_T, EDATA_T>) nbr.dec();
    }
}
