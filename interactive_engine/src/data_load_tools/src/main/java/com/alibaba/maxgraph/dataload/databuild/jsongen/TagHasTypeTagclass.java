package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class TagHasTypeTagclass extends EdgeInfo {
    String label = "hasType";
    String srcLabel = "tag";
    String dstLabel = "tagclass";

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
