package com.alibaba.maxgraph.databuild.jsongen;

public class PostIsLocatedInPlace extends EdgeInfo {
    String label = "isLocatedIn";
    String srcLabel = "post";
    String dstLabel = "place";

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
