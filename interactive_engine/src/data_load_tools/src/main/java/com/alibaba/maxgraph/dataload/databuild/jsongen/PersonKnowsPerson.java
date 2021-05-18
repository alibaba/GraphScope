package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class PersonKnowsPerson extends EdgeInfo {
    String label = "knows";
    String srcLabel = "person";
    String dstLabel = "person";

    String[] propertyNames = new String[] {
            "creationDate"
    };

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
        return propertyNames;
    }
}
