package com.alibaba.graphscope.groot.sdk.schema;

import com.alibaba.graphscope.proto.groot.PropertyDefPb;
import com.alibaba.graphscope.proto.groot.TypeDefPb;
import com.alibaba.graphscope.proto.groot.TypeEnumPb;

import java.util.ArrayList;
import java.util.List;

public class VertexLabel extends Label {
    private VertexLabel(String label, List<Property> properties, String comment) {
        this.label = label;
        this.properties = properties;
        this.comment = comment;
        this.type = TypeEnumPb.VERTEX;
    }

    private VertexLabel() {}

    public static VertexLabel fromProto(TypeDefPb proto) {
        VertexLabel label = new VertexLabel();
        label.label = proto.getLabel();
        List<Property> properties = new ArrayList<>();
        for (PropertyDefPb propertyDefPb : proto.getPropsList()) {
            properties.add(Property.fromProto(propertyDefPb));
        }
        label.properties = properties;
        label.comment = proto.getComment();
        label.type = TypeEnumPb.VERTEX;
        label.id = proto.getLabelId().getId();
        return label;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("VertexLabel {\n");
        builder.append("  label='").append(label);
        builder.append("', id=").append(id);
        builder.append(", comment='").append(comment);
        builder.append("', properties={\n");
        for (Property prop : properties) {
            builder.append("    ").append(prop.toString()).append("\n");
        }
        builder.append("  }\n}");
        return builder.toString();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String label;
        private List<Property> properties;

        private String comment;

        public Builder() {
            this.properties = new ArrayList<>();
        }

        public Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder addProperty(Property.Builder property) {
            properties.add(property.build());
            return this;
        }

        public Builder addProperty(Property property) {
            properties.add(property);
            return this;
        }

        public Builder addAllProperties(List<Property> properties) {
            properties.addAll(properties);
            return this;
        }

        public VertexLabel build() {
            return new VertexLabel(label, properties, comment);
        }
    }
}
