package com.alibaba.maxgraph.databuild.jsongen;

public class PersonLikesComment extends EdgeInfo {
    String label = "likes";
    String srcLabel = "person";
    String dstLabel = "comment";

    String[] propertyNames = new String[] {
            "creationDate"
    };

    public String getLabel() {
        return label;
    }

    public String getSrcLabel() {
        return srcLabel;
    }

    public String getDstLabel() {
        return dstLabel;
    }

    @Override
    String[] getPropertyNames() {
        return propertyNames;
    }
}
