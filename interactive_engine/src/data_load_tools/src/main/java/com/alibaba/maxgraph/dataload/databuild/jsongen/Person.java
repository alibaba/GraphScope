package com.alibaba.maxgraph.dataload.databuild.jsongen;

public class Person extends VertexInfo {
    String label = "person";

    String[] propertyNames = new String[] {
            "id",
            "firstName",
            "lastName",
            "gender",
            "birthday",
            "creationDate",
            "locationIP",
            "browserUsed"
    };

    public String getLabel() {
        return label;
    }

    public String[] getPropertyNames() {
        return propertyNames;
    }
}
