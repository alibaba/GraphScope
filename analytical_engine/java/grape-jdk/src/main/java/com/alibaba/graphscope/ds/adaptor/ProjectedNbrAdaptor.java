package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.graphscope.ds.ProjectedNbr;
import com.alibaba.graphscope.ds.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProjectedNbrAdaptor<VID_T, EDATA_T> implements Nbr<VID_T, EDATA_T> {
    private static Logger logger = LoggerFactory.getLogger(ProjectedNbrAdaptor.class.getName());

    public static final String TYPE = "ProjectedNbr";
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
        nbr = nbr.inc();
        return this;
    }

    @Override
    public boolean eq(Nbr<VID_T, EDATA_T> rhs) {
        if (!(rhs instanceof ProjectedNbrAdaptor)) {
            logger.error("rhs:" + rhs + "not instance of ProjectedNbrAdaptor");
            return false;
        }
        return nbr.eq(((ProjectedNbrAdaptor<VID_T, EDATA_T>) rhs).nbr);
    }

    @Override
    public Nbr<VID_T, EDATA_T> dec() {
        nbr.dec();
        return this;
    }
}
