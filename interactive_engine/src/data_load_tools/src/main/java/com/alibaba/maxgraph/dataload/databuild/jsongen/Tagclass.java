package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class Tagclass extends VertexInfo {
    String label = "tagclass";

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
