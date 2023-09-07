package com.alibaba.graphscope.groot.sdk.schema;

import com.alibaba.graphscope.proto.groot.DataTypePb;
import com.alibaba.graphscope.proto.groot.PropertyDefPb;

public class Property {
    private String name;

    private int id;
    private DataTypePb dataType;

    private boolean isPrimaryKey;

    private String comment;

    public String getName() {
        return name;
    }

    public DataTypePb getDataType() {
        return dataType;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public String getComment() {
        return comment;
    }

    public int getId() {
        return id;
    }

    private Property(String name, DataTypePb dataType, boolean isPrimaryKey, String comment) {
        this.name = name;
        this.dataType = dataType;
        this.isPrimaryKey = isPrimaryKey;
        this.comment = comment;
    }

    private Property() {}

    public static Property fromProto(PropertyDefPb proto) {
        Property property = new Property();
        property.name = proto.getName();
        property.dataType = proto.getDataType();
        property.isPrimaryKey = proto.getPk();
        property.comment = proto.getComment();
        property.id = proto.getId();
        return property;
    }

    @Override
    public String toString() {
        return "Property{"
                + "name='"
                + name
                + '\''
                + ", id="
                + id
                + ", dataType="
                + dataType
                + ", isPrimaryKey="
                + isPrimaryKey
                + ", comment='"
                + comment
                + '\''
                + '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private DataTypePb dataType;

        private boolean isPrimaryKey;

        private String comment;

        public Builder() {}

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setDataType(DataTypePb type) {
            this.dataType = type;
            return this;
        }

        public Builder setPrimaryKey() {
            this.isPrimaryKey = true;
            return this;
        }

        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Property build() {
            return new Property(name, dataType, isPrimaryKey, comment);
        }
    }

    public PropertyDefPb toProto() {
        PropertyDefPb.Builder builder = PropertyDefPb.newBuilder();
        builder.setName(name);
        builder.setDataType(dataType);
        builder.setPk(isPrimaryKey);
        if (comment != null) {
            builder.setComment(comment);
        }

        return builder.build();
    }
}
