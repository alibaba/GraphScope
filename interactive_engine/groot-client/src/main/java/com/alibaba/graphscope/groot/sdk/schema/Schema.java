package com.alibaba.graphscope.groot.sdk.schema;

import com.alibaba.graphscope.proto.groot.*;
import com.alibaba.graphscope.proto.groot.BatchSubmitRequest.DDLRequest;

import java.util.*;

public class Schema {
    List<VertexLabel> vertexLabels;
    List<EdgeLabel> edgeLabels;

    List<VertexLabel> vertexLabelsToDrop;
    List<EdgeLabel> edgeLabelsToDrop;

    List<VertexLabel> vertexLabelsToAddProperties;
    List<EdgeLabel> edgeLabelsToAddProperties;

    public Schema(
            List<VertexLabel> vertexLabels,
            List<EdgeLabel> edgeLabels,
            List<VertexLabel> vertexLabelsToDrop,
            List<EdgeLabel> edgeLabelsToDrop,
            List<VertexLabel> vertexLabelsToAddProperties,
            List<EdgeLabel> edgeLabelsToAddProperties) {
        this.vertexLabels = vertexLabels;
        this.edgeLabels = edgeLabels;
        this.vertexLabelsToDrop = vertexLabelsToDrop;
        this.edgeLabelsToDrop = edgeLabelsToDrop;
        this.vertexLabelsToAddProperties = vertexLabelsToAddProperties;
        this.edgeLabelsToAddProperties = edgeLabelsToAddProperties;
    }

    public Schema() {}

    /**
     * Get vertex label by label name.
     * @param name
     * @return The corresponding vertex label class if exists.
     */
    public VertexLabel getVertexLabel(String name) {
        for (VertexLabel label : vertexLabels) {
            if (label.getLabel().equals(name)) {
                return label;
            }
        }
        return null;
    }
    /**
     * Get edge label by label name.
     * @param name
     * @return The corresponding edge label class if exists.
     */
    public EdgeLabel getEdgeLabel(String name) {
        for (EdgeLabel label : edgeLabels) {
            if (label.getLabel().equals(name)) {
                return label;
            }
        }
        return null;
    }

    /**
     * Get all vertex labels.
     * @return list of vertex labels
     */
    public List<VertexLabel> getVertexLabels() {
        return vertexLabels;
    }

    /**
     * Get all edge labels.
     * @return list of vertex labels
     */
    public List<EdgeLabel> getEdgeLabels() {
        return edgeLabels;
    }

    public static Schema fromGraphDef(GraphDefPb proto) {
        Builder builder = newBuilder();

        for (TypeDefPb typeDefPb : proto.getTypeDefsList()) {
            if (typeDefPb.getTypeEnum() == TypeEnumPb.VERTEX) {
                builder.addVertexLabel(VertexLabel.fromProto(typeDefPb));
            } else {
                builder.addEdgeLabel(EdgeLabel.fromProto(typeDefPb, proto.getEdgeKindsList()));
            }
        }

        return builder.build();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Graph Schema:");
        for (VertexLabel label : vertexLabels) {
            builder.append("\n");
            builder.append(label.toString());
        }
        for (EdgeLabel label : edgeLabels) {
            builder.append("\n");
            builder.append(label.toString());
        }
        return builder.toString();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public BatchSubmitRequest toProto() {
        BatchSubmitRequest.Builder builder = BatchSubmitRequest.newBuilder();
        for (VertexLabel label : vertexLabels) {
            CreateVertexTypeRequest.Builder typeBuilder = CreateVertexTypeRequest.newBuilder();
            typeBuilder.setTypeDef(label.toProto());
            DDLRequest.Builder ddlRequestBuilder =
                    DDLRequest.newBuilder().setCreateVertexTypeRequest(typeBuilder);
            builder.addValue(ddlRequestBuilder);
        }
        for (VertexLabel label : vertexLabelsToDrop) {
            DropVertexTypeRequest.Builder typeBuilder = DropVertexTypeRequest.newBuilder();
            typeBuilder.setLabel(label.getLabel());
            DDLRequest.Builder ddlRequestBuilder =
                    DDLRequest.newBuilder().setDropVertexTypeRequest(typeBuilder);
            builder.addValue(ddlRequestBuilder);
        }
        for (VertexLabel label : vertexLabelsToAddProperties) {
            AddVertexTypePropertiesRequest.Builder typeBuilder = AddVertexTypePropertiesRequest.newBuilder();
            typeBuilder.setTypeDef(label.toProto());
            DDLRequest.Builder ddlRequestBuilder =
                    DDLRequest.newBuilder().setAddVertexTypePropertiesRequest(typeBuilder);
            builder.addValue(ddlRequestBuilder);
        }
        for (EdgeLabel label : edgeLabels) {
            CreateEdgeTypeRequest.Builder typeBuilder = CreateEdgeTypeRequest.newBuilder();
            typeBuilder.setTypeDef(label.toProto());
            DDLRequest.Builder ddlRequestBuilder =
                    DDLRequest.newBuilder().setCreateEdgeTypeRequest(typeBuilder);
            builder.addValue(ddlRequestBuilder);

            for (EdgeLabel.EdgeRelation relation : label.getRelations()) {
                AddEdgeKindRequest.Builder kindBuilder = AddEdgeKindRequest.newBuilder();
                kindBuilder.setEdgeLabel(relation.getEdgeLabel());
                kindBuilder.setSrcVertexLabel(relation.getSrcLabel());
                kindBuilder.setDstVertexLabel(relation.getDstLabel());
                builder.addValue(DDLRequest.newBuilder().setAddEdgeKindRequest(kindBuilder));
            }
        }
        for (EdgeLabel label : edgeLabelsToAddProperties) {
            AddEdgeTypePropertiesRequest.Builder typeBuilder = AddEdgeTypePropertiesRequest.newBuilder();
            typeBuilder.setTypeDef(label.toProto());
            DDLRequest.Builder ddlRequestBuilder =
                    DDLRequest.newBuilder().setAddEdgeTypePropertiesRequest(typeBuilder);
            builder.addValue(ddlRequestBuilder);
        }
        for (EdgeLabel label : edgeLabelsToDrop) {
            for (EdgeLabel.EdgeRelation relation : label.getRelations()) {
                RemoveEdgeKindRequest.Builder kindBuilder = RemoveEdgeKindRequest.newBuilder();
                kindBuilder.setEdgeLabel(relation.getEdgeLabel());
                kindBuilder.setSrcVertexLabel(relation.getSrcLabel());
                kindBuilder.setDstVertexLabel(relation.getDstLabel());
                builder.addValue(DDLRequest.newBuilder().setRemoveEdgeKindRequest(kindBuilder));
            }

            if (label.getRelations().isEmpty()) {
                DropEdgeTypeRequest.Builder typeBuilder = DropEdgeTypeRequest.newBuilder();
                typeBuilder.setLabel(label.getLabel());
                DDLRequest.Builder ddlRequestBuilder =
                        DDLRequest.newBuilder().setDropEdgeTypeRequest(typeBuilder);
                builder.addValue(ddlRequestBuilder);
            }
        }
        return builder.build();
    }

    public static class Builder {
        List<VertexLabel> vertexLabels;
        List<EdgeLabel> edgeLabels;

        List<VertexLabel> vertexLabelsToDrop;
        List<EdgeLabel> edgeLabelsToDrop;

        List<VertexLabel> vertexLabelsToAddProperties;
        List<EdgeLabel> edgeLabelsToAddProperties;

        public Builder() {
            vertexLabels = new ArrayList<>();
            edgeLabels = new ArrayList<>();
            vertexLabelsToDrop = new ArrayList<>();
            edgeLabelsToDrop = new ArrayList<>();
            vertexLabelsToAddProperties = new ArrayList<>();
            edgeLabelsToAddProperties = new ArrayList<>();
        }

        public Builder addVertexLabel(VertexLabel label) {
            vertexLabels.add(label);
            return this;
        }

        public Builder addVertexLabelProperties(VertexLabel label) {
            if (vertexLabelsToAddProperties.stream().anyMatch(item -> item.getLabel().equals(label.getLabel()))) {
                throw new IllegalArgumentException(label.getLabel() + " duplicated label in submission queue. " +
                        "merge all properties if they belong to same label.");
            }
            vertexLabelsToAddProperties.add(label);
            return this;
        }

        public Builder addEdgeLabel(EdgeLabel label) {
            edgeLabels.add(label);
            return this;
        }

        public Builder addEdgeLabelProperties(EdgeLabel label) {
            if (edgeLabelsToAddProperties.stream().anyMatch(item -> item.getLabel().equals(label.getLabel()))) {
                throw new IllegalArgumentException(label.getLabel() + " duplicated label in submission queue. " +
                        "merge all properties if they belong to same label.");
            }
            edgeLabelsToAddProperties.add(label);
            return this;
        }

        public Builder dropVertexLabel(VertexLabel label) {
            vertexLabelsToDrop.add(label);
            return this;
        }

        /**
         * Drop edge kind if there is any relations in the EdgeLabel.
         * If there is no relation, then drop the edge label.
         * @param label
         * @return
         */
        public Builder dropEdgeLabelOrKind(EdgeLabel label) {
            edgeLabelsToDrop.add(label);
            return this;
        }

        public Builder addVertexLabel(VertexLabel.Builder label) {
            return addVertexLabel(label.build());
        }

        public Builder addVertexLabelProperties(VertexLabel.Builder label) {
            return addVertexLabelProperties(label.build());
        }

        public Builder addEdgeLabel(EdgeLabel.Builder label) {
            return addEdgeLabel(label.build());
        }

        public Builder addEdgeLabelProperties(EdgeLabel.Builder label) {
            return addEdgeLabelProperties(label.build());
        }

        public Builder dropVertexLabel(VertexLabel.Builder label) {
            return dropVertexLabel(label.build());
        }

        /**
         * Drop edge kind if there is any relations in the EdgeLabel.
         * If there is no relation, then drop the edge label.
         * @param label
         * @return
         */
        public Builder dropEdgeLabelOrKind(EdgeLabel.Builder label) {
            return dropEdgeLabelOrKind(label.build());
        }

        public Schema build() {
            return new Schema(vertexLabels, edgeLabels, vertexLabelsToDrop, edgeLabelsToDrop,
                    vertexLabelsToAddProperties, edgeLabelsToAddProperties);
        }
    }
}
