package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class Comment extends VertexInfo {
    String label = "comment";

    String[] propertyNames = new String[] {
            "id",
            "creationDate",
            "locationIP",
            "browserUsed",
            "content",
            "length"
    };

    public String getLabel() {
        return label;
    }

    public String[] getPropertyNames() {
        return propertyNames;
    }
}
