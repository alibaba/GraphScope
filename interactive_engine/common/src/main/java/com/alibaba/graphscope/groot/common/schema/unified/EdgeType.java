package com.alibaba.graphscope.groot.common.schema.unified;

import com.alibaba.graphscope.groot.common.schema.api.EdgeRelation;
import com.alibaba.graphscope.groot.common.schema.api.GraphEdge;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.List;

public class EdgeType extends Type implements GraphEdge {
    public List<VertexTypePairRelation> vertexTypePairRelations;

    @Override
    public String toString() {
        return "EdgeType{"
                + "vertexTypePairRelations="
                + vertexTypePairRelations
                + ", typeId="
                + typeId
                + ", typeName='"
                + typeName
                + '\''
                + ", description='"
                + description
                + '\''
                + ", primaryKeys="
                + primaryKeys
                + ", properties="
                + properties
                + '}';
    }

    @Override
    @JsonIgnore
    public List<EdgeRelation> getRelationList() {
        return new ArrayList<>(vertexTypePairRelations);
    }
}
