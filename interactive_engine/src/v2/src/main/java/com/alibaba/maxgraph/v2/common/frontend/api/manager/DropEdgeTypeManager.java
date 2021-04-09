package com.alibaba.maxgraph.v2.common.frontend.api.manager;

public interface DropEdgeTypeManager extends SchemaManager {
    /**
     * Get the edge type
     *
     * @return The edge type
     */
    String getLabel();
}
