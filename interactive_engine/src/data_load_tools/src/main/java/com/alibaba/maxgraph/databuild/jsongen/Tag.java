package com.alibaba.maxgraph.databuild.jsongen;

public class Tag extends VertexInfo {
    String label = "tag";

    String[] propertyNames = new String[] {
            "id",
            "name",
            "url"
    };

    public String getLabel() {
        return label;
    }

    public String[] getPropertyNames() {
        return propertyNames;
    }
}
