package com.alibaba.graphscope.groot.sdk.schema;

import com.alibaba.graphscope.proto.groot.TypeEnumPb;

import java.util.ArrayList;
import java.util.List;

public class EdgeLabel extends Label {
    private List<EdgeRelation> relations;

    private EdgeLabel(
            String label, List<Property> properties, List<EdgeRelation> relations, String comment) {
        this.label = label;
        this.properties = properties;
        this.relations = relations;
        this.comment = comment;
        this.type = TypeEnumPb.EDGE;
    }

    public List<EdgeRelation> getRelations() {
        return relations;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String label;
        private List<Property> properties;
        private List<EdgeRelation> relations;

        private String comment;

        public Builder() {
            this.properties = new ArrayList<>();
            this.relations = new ArrayList<>();
        }

        public Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder addRelation(String srcLabel, String dstLabel) {
            relations.add(new EdgeRelation(label, srcLabel, dstLabel));
            return this;
        }

        public Builder addProperty(Property property) {
            properties.add(property);
            return this;
        }

        public Builder addProperty(Property.Builder property) {
            properties.add(property.build());
            return this;
        }

        public Builder addAllProperties(List<Property> properties) {
            properties.addAll(properties);
            return this;
        }

        public EdgeLabel build() {
            return new EdgeLabel(label, properties, relations, comment);
        }
    }

    public static class EdgeRelation {
        private String edgeLabel;
        private String srcLabel;
        private String dstLabel;

        public String getEdgeLabel() {
            return edgeLabel;
        }

        public String getSrcLabel() {
            return srcLabel;
        }

        public String getDstLabel() {
            return dstLabel;
        }

        public EdgeRelation(String edgeLabel, String srcLabel, String dstLabel) {
            this.edgeLabel = edgeLabel;
            this.srcLabel = srcLabel;
            this.dstLabel = dstLabel;
        }
    }
}
