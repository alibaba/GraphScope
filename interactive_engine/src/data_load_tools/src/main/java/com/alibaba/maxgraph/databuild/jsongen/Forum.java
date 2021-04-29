package com.alibaba.maxgraph.databuild.jsongen;

public class Forum extends VertexInfo {
    String label = "forum";

    String[] propertyNames = new String[] {
            "id",
            "title",
            "creationDate"
    };

    public String getLabel() {
        return label;
    }

    public String[] getPropertyNames() {
        return propertyNames;
    }
}
