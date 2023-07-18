package com.alibaba.graphscope.groot.sdk.schema;

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
