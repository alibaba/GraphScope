package com.alibaba.maxgraph.v2.frontend.graph.memory.schema.mapper;

import java.util.List;

public abstract class SchemaElementMapper {
    private int id;
    private String label;
    private String type;
    private List<GraphPropertyMapper> propertyDefList;
    private int versionId;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<GraphPropertyMapper> getPropertyDefList() {
        return propertyDefList;
    }

    public void setPropertyDefList(List<GraphPropertyMapper> propertyDefList) {
        this.propertyDefList = propertyDefList;
    }

    public int getVersionId() {
        return versionId;
    }

    public void setVersionId(int versionId) {
        this.versionId = versionId;
    }
}
