package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class CommentReplyOfComment extends EdgeInfo {
    String label = "replyOf";
    String srcLabel = "comment";
    String dstLabel = "comment";

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
