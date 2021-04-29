package com.alibaba.maxgraph.databuild.jsongen;

public class PostHasTagTag extends EdgeInfo {
    String label = "hasTag";
    String srcLabel = "post";
    String dstLabel = "tag";

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
