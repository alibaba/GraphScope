package com.alibaba.maxgraph.databuild.jsongen;

public class PostHasCreatorPerson extends EdgeInfo {
    String label = "hasCreator";
    String srcLabel = "post";
    String dstLabel = "person";

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
        return new String[0];
    }
}
