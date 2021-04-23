package com.alibaba.maxgraph.v2.common.frontend.api.manager;

public interface DropVertexTypeManager extends SchemaManager {
    /**
     * get the vertex type
     *
     * @return The vertex type
     */
    String getLabel();
}
