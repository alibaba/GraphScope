package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class Place extends VertexInfo {
    String label = "place";

    String[] propertyNames = new String[] {
            "id",
            "name",
            "url",
            "type",
    };

    public String getLabel() {
        return label;
    }

    public String[] getPropertyNames() {
        return propertyNames;
    }
}
