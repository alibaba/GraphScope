package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.graphscope.ds.GrapeNbr;
import com.alibaba.graphscope.ds.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrapeNbrAdaptor<VID_T, EDATA_T> implements Nbr<VID_T, EDATA_T> {
    private static Logger logger = LoggerFactory.getLogger(GrapeNbrAdaptor.class.getName());
    public static final String TYPE = "GrapeNbr";
    private GrapeNbr<VID_T, EDATA_T> nbr;

    @Override
    public String type() {
        return TYPE;
    }

    public GrapeNbrAdaptor(GrapeNbr<VID_T, EDATA_T> n) {
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
        logger.error("No implementation for inc in grapeNbr");
        return null;
    }

    @Override
    public boolean eq(Nbr<VID_T, EDATA_T> rhs) {
        logger.error("No implementation for eq in grapeNbr");
        return false;
    }

    @Override
    public Nbr<VID_T, EDATA_T> dec() {
        logger.error("No implementation for dec in grapeNbr");
        return null;
    }
}
