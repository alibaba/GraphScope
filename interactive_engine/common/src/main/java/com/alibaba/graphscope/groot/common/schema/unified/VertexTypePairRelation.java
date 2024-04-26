package com.alibaba.graphscope.groot.common.schema.unified;

import com.alibaba.graphscope.groot.common.schema.api.EdgeRelation;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class VertexTypePairRelation implements EdgeRelation {
    public String sourceVertex;
    public String destinationVertex;

    private long tableId;

    @JsonIgnore public GraphVertex source;
    @JsonIgnore public GraphVertex target;

    @Override
    public String toString() {
        return "VertexTypePairRelation{"
                + "sourceVertex='"
                + sourceVertex
                + '\''
                + ", destinationVertex='"
                + destinationVertex
                + '\''
                + '}';
    }

    @Override
    public GraphVertex getSource() {
        return source;
    }

    @Override
    public GraphVertex getTarget() {
        return target;
    }

    @Override
    public long getTableId() {
        return tableId;
    }
}
