package com.alibaba.maxgraph.v2.common.frontend.api.graph;

public interface GraphPartitionManager {
    int getVertexStoreId(int labelId, long vertexId);

    int getEdgeStoreId(int srcLabelId, long srcVertexId, int edgeLabelId, long edgeId, int dstLabelId, long dstVertexId);
}
