package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class Post extends VertexInfo {
    String label = "post";

    String[] propertyNames = new String[] {
            "id",
            "imageFile",
            "creationDate",
            "locationIP",
            "browserUsed",
            "language",
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
