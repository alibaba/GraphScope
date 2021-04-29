package com.alibaba.maxgraph.databuild.jsongen;

public class ForumHasMemberPerson extends EdgeInfo {
    String label = "hasMember";
    String srcLabel = "forum";
    String dstLabel = "person";

    String[] propertyNames = new String[] {
            "joinDate"
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
