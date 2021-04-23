package com.alibaba.maxgraph.v2.frontend.graph.memory.schema.mapper;

import java.util.List;

public class VertexIndexMapper {
    private String name;
    private String indexType;
    private List<String> propertyNames;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public List<String> getPropertyNames() {
        return propertyNames;
    }

    public void setPropertyNames(List<String> propertyNames) {
        this.propertyNames = propertyNames;
    }
}
