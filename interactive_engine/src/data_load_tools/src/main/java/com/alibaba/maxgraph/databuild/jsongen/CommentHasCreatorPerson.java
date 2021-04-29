package com.alibaba.maxgraph.databuild.jsongen;

public class CommentHasCreatorPerson extends EdgeInfo {
    String label = "hasCreator";
    String srcLabel = "comment";
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
