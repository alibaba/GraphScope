package com.alibaba.graphscope.groot.sdk.schema;

import com.alibaba.graphscope.proto.groot.EdgeKindPb;
import com.alibaba.graphscope.proto.groot.PropertyDefPb;
import com.alibaba.graphscope.proto.groot.TypeDefPb;
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

    private EdgeLabel() {}

    public List<EdgeRelation> getRelations() {
        return relations;
    }

    public static EdgeLabel fromProto(TypeDefPb proto, List<EdgeKindPb> edgeKindPbs) {
        EdgeLabel label = new EdgeLabel();
        label.label = proto.getLabel();
        List<Property> properties = new ArrayList<>();
        for (PropertyDefPb propertyDefPb : proto.getPropsList()) {
            properties.add(Property.fromProto(propertyDefPb));
        }
        label.properties = properties;
        label.comment = proto.getComment();
        label.type = TypeEnumPb.EDGE;
        label.id = proto.getLabelId().getId();

        List<EdgeRelation> relations = new ArrayList<>();
        for (EdgeKindPb edgeKindPb : edgeKindPbs) {
            String edgeLabel = edgeKindPb.getEdgeLabel();
            String srcLabel = edgeKindPb.getSrcVertexLabel();
            String dstLabel = edgeKindPb.getDstVertexLabel();
            if (edgeLabel.equals(proto.getLabel())) {
                relations.add(new EdgeRelation(edgeLabel, srcLabel, dstLabel));
            }
        }
        label.relations = relations;
        return label;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("EdgeLabel {\n");
        builder.append("  label='").append(label);
        builder.append("', id=").append(id);
        builder.append(", comment='").append(comment);
        builder.append("', properties={\n");
        for (Property prop : properties) {
            builder.append("    ").append(prop.toString()).append("\n");
        }
        builder.append("  }\n");
        builder.append("  relations={\n");
        for (EdgeRelation relation : relations) {
            builder.append("    ").append(relation.toString()).append("\n");
        }
        builder.append("  }\n}");
        return builder.toString();
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

        private EdgeRelation(String edgeLabel, String srcLabel, String dstLabel) {
            this.edgeLabel = edgeLabel;
            this.srcLabel = srcLabel;
            this.dstLabel = dstLabel;
        }

        @Override
        public String toString() {
            return "EdgeRelation{"
                    + "srcLabel='"
                    + srcLabel
                    + '\''
                    + ", dstLabel='"
                    + dstLabel
                    + '\''
                    + '}';
        }
    }
}
