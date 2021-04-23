package com.alibaba.maxgraph.v2.common.frontend.api.schema;

import java.util.List;

/**
 * Graph edge
 */
public interface EdgeType extends SchemaElement {
    /**
     * The edge relation list
     * @return The relation list
     */
    List<EdgeRelation> getRelationList();
}
