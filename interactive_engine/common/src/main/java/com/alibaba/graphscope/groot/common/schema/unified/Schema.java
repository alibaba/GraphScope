package com.alibaba.graphscope.groot.common.schema.unified;

import com.alibaba.graphscope.groot.common.exception.TypeNotFoundException;

import java.util.List;
import java.util.Objects;

public class Schema {
    public List<VertexType> vertexTypes;
    public List<EdgeType> edgeTypes;

    @Override
    public String toString() {
        return "Schema{" + "vertexTypes=" + vertexTypes + ", edgeTypes=" + edgeTypes + '}';
    }

    private VertexType getVertexType(String label) throws TypeNotFoundException {
        for (VertexType type : vertexTypes) {
            if (Objects.equals(type.getLabel(), label)) {
                return type;
            }
        }
        return null;
    }

    public Schema munge() {
        for (EdgeType edgeType : edgeTypes) {
            for (VertexTypePairRelation relation : edgeType.vertexTypePairRelations) {
                relation.source = getVertexType(relation.sourceVertex);
                relation.target = getVertexType(relation.destinationVertex);
            }
        }
        return this;
    }
}
