package com.alibaba.maxgraph.databuild.jsongen;

public class CommentReplyOfPost extends EdgeInfo {
    String label = "replyOf";
    String srcLabel = "comment";
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
