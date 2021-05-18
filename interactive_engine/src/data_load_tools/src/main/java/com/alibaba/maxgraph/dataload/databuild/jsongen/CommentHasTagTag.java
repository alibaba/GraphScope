package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class CommentHasTagTag extends EdgeInfo {
    String label = "hasTag";
    String srcLabel = "comment";
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
