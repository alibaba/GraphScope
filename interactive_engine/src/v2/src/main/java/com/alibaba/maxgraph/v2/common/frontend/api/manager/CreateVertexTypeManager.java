package com.alibaba.maxgraph.v2.common.frontend.api.manager;

import java.util.List;

/**
 * The interface of create vertex type manager
 */
public interface CreateVertexTypeManager extends CreateElementTypeManager<CreateVertexTypeManager> {
    /**
     * Specify the primary key list for the vertex type
     *
     * @param primaryKeys The primary key list
     * @return The type manager
     */
    CreateVertexTypeManager primaryKey(String... primaryKeys);

    /**
     * Get primary key list for vertex type
     *
     * @return The primary key list
     */
    List<String> getPrimaryKeyList();
}
