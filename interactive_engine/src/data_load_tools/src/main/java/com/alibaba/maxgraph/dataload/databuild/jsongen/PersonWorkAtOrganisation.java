package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class PersonWorkAtOrganisation extends EdgeInfo {
    String label = "workAt";
    String srcLabel = "person";
    String dstLabel = "organisation";

    String[] propertyNames = new String[] {
            "workFrom"
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
