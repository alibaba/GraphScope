package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.fastffi.impl.CXXStdString;
import com.alibaba.graphscope.ds.ProjectedNbrStrData;
import com.alibaba.graphscope.ds.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProjectedNbrStrDataAdaptor<VID_T> implements Nbr<VID_T, CXXStdString> {
    private static Logger logger =
            LoggerFactory.getLogger(ProjectedNbrStrDataAdaptor.class.getName());

    public static final String TYPE = "ProjectedNbrStrDataAdaptor";
    private ProjectedNbrStrData<VID_T> nbr;

    @Override
    public String type() {
        return TYPE;
    }

    public ProjectedNbrStrDataAdaptor(ProjectedNbrStrData<VID_T> n) {
        nbr = n;
    }

    @Override
    public Vertex<VID_T> neighbor() {
        return nbr.neighbor();
    }

    @Override
    public CXXStdString data() {
        return nbr.data();
    }

    @Override
    public Nbr<VID_T, CXXStdString> inc() {
        nbr = nbr.inc();
        return this;
    }

    @Override
    public boolean eq(Nbr<VID_T, CXXStdString> rhs) {
        if (!(rhs instanceof ProjectedNbrStrDataAdaptor)) {
            logger.error("rhs:" + rhs + "not instance of ProjectedNbrAdaptor");
            return false;
        }
        return nbr.eq(((ProjectedNbrStrDataAdaptor<VID_T>) rhs).nbr);
    }

    @Override
    public Nbr<VID_T, CXXStdString> dec() {
        nbr.dec();
        return this;
    }
}
