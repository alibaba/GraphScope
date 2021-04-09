package com.alibaba.maxgraph.v2.frontend.graph.memory.schema.mapper;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultGraphProperty;

public class GraphPropertyMapper {
    private int id;
    private String name;
    private String dataType;
    private String comment;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public static GraphPropertyMapper parseFromGrapyProperty(GraphProperty graphProperty) {
        GraphPropertyMapper propertyMapper = new GraphPropertyMapper();
        propertyMapper.setId(graphProperty.getId());
        propertyMapper.setName(graphProperty.getName());
        propertyMapper.setComment(graphProperty.getComment());
        propertyMapper.setDataType(graphProperty.getDataType().toString());
        return propertyMapper;
    }

    public GraphProperty toGraphProperty() {
        return new DefaultGraphProperty(this.name, this.id, DataType.parseString(this.dataType));
    }
}
