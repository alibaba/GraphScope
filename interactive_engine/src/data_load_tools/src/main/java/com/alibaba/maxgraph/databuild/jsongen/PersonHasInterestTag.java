package com.alibaba.maxgraph.databuild.jsongen;

public class PersonHasInterestTag extends EdgeInfo {
    String label = "hasInterest";
    String srcLabel = "person";
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
