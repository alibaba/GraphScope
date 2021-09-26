package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;

public interface VertexDataManager<VDATA_T extends Writable> {

    VDATA_T getVertexData(long lid);

    void setVertexData(long lid, VDATA_T vertexData);
}
