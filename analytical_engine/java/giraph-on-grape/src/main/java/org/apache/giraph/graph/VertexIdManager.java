package org.apache.giraph.graph;

import org.apache.hadoop.io.WritableComparable;

public interface VertexIdManager<OID_T extends WritableComparable> {

    OID_T getId(long lid);
}
