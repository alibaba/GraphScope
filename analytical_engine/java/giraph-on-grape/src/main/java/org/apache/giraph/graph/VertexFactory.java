package org.apache.giraph.graph;

import com.alibaba.graphscope.context.GiraphComputationAdaptorContext;

import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class VertexFactory {

    public static <
                    OID_T extends WritableComparable,
                    VDATA_T extends Writable,
                    EDATA_T extends Writable>
            VertexImpl<OID_T, VDATA_T, EDATA_T> createDefaultVertex(
                    Class<? extends OID_T> oidClass,
                    Class<? extends VDATA_T> vdataClass,
                    Class<? extends EDATA_T> edataClass,
                    GiraphComputationAdaptorContext context) {
        return new VertexImpl<OID_T, VDATA_T, EDATA_T>(context);
    }
}
