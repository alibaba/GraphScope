package com.alibaba.maxgraph.v2.common.frontend.api.manager;

import java.util.List;

/**
 * The interface of create edge type manager
 */
public interface CreateEdgeTypeManager extends CreateElementTypeManager<CreateEdgeTypeManager> {
    /**
     * Add relation sourceVertexLabel->edgeLabel->targetVertexLabel to given edge type
     *
     * @param sourceVertexLabel The given source vertex label
     * @param targetVertexLabel The given target vertex label
     * @return The type manager
     */
    CreateEdgeTypeManager addRelation(String sourceVertexLabel, String targetVertexLabel);

    /**
     * Get relation list from manager
     *
     * @return The result relation list
     */
    List<EdgeRelationEntity> getRelationList();
}
