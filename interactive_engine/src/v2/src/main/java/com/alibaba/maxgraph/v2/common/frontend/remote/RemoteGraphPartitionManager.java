package com.alibaba.maxgraph.v2.common.frontend.remote;

import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.frontend.api.graph.GraphPartitionManager;
import com.alibaba.maxgraph.v2.common.util.PartitionUtils;

public class RemoteGraphPartitionManager implements GraphPartitionManager {
    private MetaService metaService;

    public RemoteGraphPartitionManager(MetaService metaService) {
        this.metaService = metaService;
    }

    @Override
    public int getVertexStoreId(int labelId, long vertexId) {
        int partitionCount = this.metaService.getPartitionCount();
        int partitionId = PartitionUtils.getPartitionIdFromKey(vertexId, partitionCount);
        return this.metaService.getStoreIdByPartition(partitionId);
    }

    @Override
    public int getEdgeStoreId(int srcLabelId, long srcVertexId, int edgeLabelId, long edgeId, int dstLabelId, long dstVertexId) {
        int partitionCount = this.metaService.getPartitionCount();
        int partitionId = PartitionUtils.getPartitionIdFromKey(srcVertexId, partitionCount);
        return this.metaService.getStoreIdByPartition(partitionId);
    }
}
