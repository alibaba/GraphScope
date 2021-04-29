package com.alibaba.maxgraph.databuild.jsongen;

public class Organisation extends VertexInfo {
    String label = "organisation";

    String[] propertyNames = new String[] {
            "id",
            "type",
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
