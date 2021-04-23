package com.alibaba.maxgraph.v2.common.frontend.api.schema;

import com.alibaba.maxgraph.v2.common.schema.GraphTable;

/**
 * Relation in edge
 */
public interface EdgeRelation extends GraphTable {
    /**
     * Source vertex
     * @return The source vertex
     */
    VertexType getSource();

    /**
     * Target vertex
     * @return The target vertex
     */
    VertexType getTarget();
}
