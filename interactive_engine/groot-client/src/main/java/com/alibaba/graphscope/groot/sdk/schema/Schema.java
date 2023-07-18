package com.alibaba.graphscope.groot.sdk.schema;

import com.alibaba.graphscope.proto.groot.*;
import com.alibaba.graphscope.proto.groot.BatchSubmitRequest.DDLRequest;

import java.util.ArrayList;
import java.util.List;

public class Schema {
    List<VertexLabel> vertexLabels;
    List<EdgeLabel> edgeLabels;

    List<VertexLabel> vertexLabelsToDrop;
    List<EdgeLabel> edgeLabelsToDrop;

    public Schema(
            List<VertexLabel> vertexLabels,
            List<EdgeLabel> edgeLabels,
            List<VertexLabel> vertexLabelsToDrop,
            List<EdgeLabel> edgeLabelsToDrop) {
        this.vertexLabels = vertexLabels;
        this.edgeLabels = edgeLabels;
        this.vertexLabelsToDrop = vertexLabelsToDrop;
        this.edgeLabelsToDrop = edgeLabelsToDrop;
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

        public Builder() {
            vertexLabels = new ArrayList<>();
            edgeLabels = new ArrayList<>();
            vertexLabelsToDrop = new ArrayList<>();
            edgeLabelsToDrop = new ArrayList<>();
        }

        public Builder addVertexLabel(VertexLabel label) {
            vertexLabels.add(label);
            return this;
        }

        public Builder addEdgeLabel(EdgeLabel label) {
            edgeLabels.add(label);
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

        public Builder addEdgeLabel(EdgeLabel.Builder label) {
            return addEdgeLabel(label.build());
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
            return new Schema(vertexLabels, edgeLabels, vertexLabelsToDrop, edgeLabelsToDrop);
        }
    }
}
