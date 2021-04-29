package com.alibaba.maxgraph.databuild.jsongen;

public class ForumContainerOfPost extends EdgeInfo {
    String label = "containerOf";
    String srcLabel = "forum";
    String dstLabel = "post";

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
