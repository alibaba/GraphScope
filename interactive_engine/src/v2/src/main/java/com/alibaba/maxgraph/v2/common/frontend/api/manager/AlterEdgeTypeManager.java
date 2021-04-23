package com.alibaba.maxgraph.v2.common.frontend.api.manager;

import java.util.List;

/**
 * Alter edge type interface, contains add/drop properties and add/drop relations
 */
public interface AlterEdgeTypeManager extends AlterElementTypeManager<AlterEdgeTypeManager> {
    /**
     * Add relation sourceVertexLabel->edgeLabel->targetVertexLabel to given edge type
     *
     * @param sourceVertexLabel The given source vertex label
     * @param targetVertexLabel The given target vertex label
     * @return The type manager
     */
    AlterEdgeTypeManager addRelation(String sourceVertexLabel, String targetVertexLabel);

    /**
     * Drop relation sourceVertexLabel->edgeLabel->targetVertexLabel from given edge type
     *
     * @param sourceVertexLabel The given source vertex label
     * @param targetVertexLabel The given target vertex label
     * @return The type manager
     */
    AlterEdgeTypeManager dropRelation(String sourceVertexLabel, String targetVertexLabel);

    /**
     * Get the add relation list
     *
     * @return The relation list
     */
    List<EdgeRelationEntity> getAddRelationList();

    /**
     * Get the drop relation list
     *
     * @return The relation list
     */
    List<EdgeRelationEntity> getDropRelationList();
}
