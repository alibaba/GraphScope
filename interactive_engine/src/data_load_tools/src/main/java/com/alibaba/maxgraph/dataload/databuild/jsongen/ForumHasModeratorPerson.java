package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class ForumHasModeratorPerson extends EdgeInfo {
    String label = "hasModerator";
    String srcLabel = "forum";
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
