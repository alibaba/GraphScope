package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class PersonLikesPost extends EdgeInfo {
    String label = "likes";
    String srcLabel = "person";
    String dstLabel = "post";

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
