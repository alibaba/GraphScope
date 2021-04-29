package com.alibaba.maxgraph.databuild.jsongen;

public class PersonStudyAtOrganisation extends EdgeInfo {
    String label = "studyAt";
    String srcLabel = "person";
    String dstLabel = "organisation";

    String[] propertyNames = new String[] {
            "classYear"
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
