package com.alibaba.graphscope.groot.sdk.schema;

import com.alibaba.graphscope.proto.groot.TypeDefPb;
import com.alibaba.graphscope.proto.groot.TypeEnumPb;

import java.util.List;

public class Label {
    protected String label;
    protected int id;
    protected List<Property> properties;
    protected String comment;
    protected TypeEnumPb type;

    public String getLabel() {
        return label;
    }

    public List<Property> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }

    public Property getProperty(String name) {
        for (Property prop : properties) {
            if (prop.getName().equals(name)) {
                return prop;
            }
        }
        return null;
    }

    public int getId() {
        return id;
    }

    public TypeDefPb toProto() {
        TypeDefPb.Builder builder = TypeDefPb.newBuilder();
        builder.setTypeEnum(type);
        builder.setLabel(label);
        for (Property property : properties) {
            builder.addProps(property.toProto());
        }
        if (comment != null) {
            builder.setComment(comment);
        }
        return builder.build();
    }
}
