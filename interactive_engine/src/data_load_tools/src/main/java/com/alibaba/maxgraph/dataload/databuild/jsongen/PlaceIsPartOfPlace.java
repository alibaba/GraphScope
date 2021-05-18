package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class PlaceIsPartOfPlace extends EdgeInfo {
    String label = "isPartOf";
    String srcLabel = "place";
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
