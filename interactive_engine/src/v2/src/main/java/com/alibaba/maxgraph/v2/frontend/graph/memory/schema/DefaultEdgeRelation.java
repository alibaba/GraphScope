package com.alibaba.maxgraph.v2.frontend.graph.memory.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeRelation;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.google.common.base.MoreObjects;

/**
 * Default edge relation
 */
public class DefaultEdgeRelation implements EdgeRelation {
    private VertexType source;
    private VertexType target;
    private long tableId;

    public DefaultEdgeRelation(VertexType source, VertexType target, long tableId) {
        this.source = source;
        this.target = target;
        this.tableId = tableId;
    }

    public DefaultEdgeRelation(VertexType source, VertexType target) {
        this(source, target, -1L);
    }

    @Override
    public VertexType getSource() {
        return source;
    }

    @Override
    public VertexType getTarget() {
        return target;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("source", this.getSource().getLabel())
                .add("target", this.getTarget().getLabel())
                .toString();
    }

    @Override
    public long getTableId() {
        return this.tableId;
    }
}
