package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class ForumHasTagTag extends EdgeInfo {
    String label = "hasTag";
    String srcLabel = "forum";
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
