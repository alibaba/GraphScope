package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class PersonIsLocatedInPlace extends EdgeInfo {
    String label = "isLocatedIn";
    String srcLabel = "person";
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
