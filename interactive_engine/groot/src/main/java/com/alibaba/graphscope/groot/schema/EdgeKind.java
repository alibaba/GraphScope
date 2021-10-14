/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.schema;

import com.alibaba.maxgraph.proto.groot.EdgeKindPb;
import com.alibaba.graphscope.groot.operation.LabelId;

import java.util.Objects;

public class EdgeKind {

    private String edgeLabel;
    private LabelId edgeLabelId;

    private String srcVertexLabel;
    private LabelId srcVertexLabelId;

    private String dstVertexLabel;
    private LabelId dstVertexLabelId;

    private EdgeKind(
            String edgeLabel,
            LabelId edgeLabelId,
            String srcVertexLabel,
            LabelId srcVertexLabelId,
            String dstVertexLabel,
            LabelId dstVertexLabelId) {
        this.edgeLabel = edgeLabel;
        this.edgeLabelId = edgeLabelId;
        this.srcVertexLabel = srcVertexLabel;
        this.srcVertexLabelId = srcVertexLabelId;
        this.dstVertexLabel = dstVertexLabel;
        this.dstVertexLabelId = dstVertexLabelId;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public LabelId getEdgeLabelId() {
        return edgeLabelId;
    }

    public String getSrcVertexLabel() {
        return srcVertexLabel;
    }

    public LabelId getSrcVertexLabelId() {
        return srcVertexLabelId;
    }

    public String getDstVertexLabel() {
        return dstVertexLabel;
    }

    public LabelId getDstVertexLabelId() {
        return dstVertexLabelId;
    }

    public static EdgeKind parseProto(EdgeKindPb proto) {
        String edgeLabel = proto.getEdgeLabel();
        LabelId edgeLabelId = LabelId.parseProto(proto.getEdgeLabelId());
        String srcVertexLabel = proto.getSrcVertexLabel();
        LabelId srcVertexLabelId = LabelId.parseProto(proto.getSrcVertexLabelId());
        String dstVertexLabel = proto.getDstVertexLabel();
        LabelId dstVertexLabelId = LabelId.parseProto(proto.getDstVertexLabelId());
        return new EdgeKind(
                edgeLabel,
                edgeLabelId,
                srcVertexLabel,
                srcVertexLabelId,
                dstVertexLabel,
                dstVertexLabelId);
    }

    public EdgeKindPb toProto() {
        return EdgeKindPb.newBuilder()
                .setEdgeLabel(edgeLabel)
                .setEdgeLabelId(edgeLabelId.toProto())
                .setSrcVertexLabel(srcVertexLabel)
                .setSrcVertexLabelId(srcVertexLabelId.toProto())
                .setDstVertexLabel(dstVertexLabel)
                .setDstVertexLabelId(dstVertexLabelId.toProto())
                .build();
    }

    public EdgeKindPb toOperationProto() {
        return EdgeKindPb.newBuilder()
                .setEdgeLabelId(edgeLabelId.toProto())
                .setSrcVertexLabelId(srcVertexLabelId.toProto())
                .setDstVertexLabelId(dstVertexLabelId.toProto())
                .build();
    }

    public EdgeKindPb toDdlProto() {
        return EdgeKindPb.newBuilder()
                .setEdgeLabel(edgeLabel)
                .setSrcVertexLabel(srcVertexLabel)
                .setDstVertexLabel(dstVertexLabel)
                .build();
    }

    @Override
    public String toString() {
        return "EdgeKind{"
                + "edgeLabel='"
                + edgeLabel
                + '\''
                + ", edgeLabelId="
                + edgeLabelId
                + ", srcVertexLabel='"
                + srcVertexLabel
                + '\''
                + ", srcVertexLabelId="
                + srcVertexLabelId
                + ", dstVertexLabel='"
                + dstVertexLabel
                + '\''
                + ", dstVertexLabelId="
                + dstVertexLabelId
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EdgeKind edgeKind = (EdgeKind) o;

        if (!Objects.equals(edgeLabelId, edgeKind.edgeLabelId)) {
            return false;
        }
        if (!Objects.equals(srcVertexLabelId, edgeKind.srcVertexLabelId)) {
            return false;
        }
        return Objects.equals(dstVertexLabelId, edgeKind.dstVertexLabelId);
    }

    @Override
    public int hashCode() {
        int result = edgeLabelId != null ? edgeLabelId.hashCode() : 0;
        result = 31 * result + (srcVertexLabelId != null ? srcVertexLabelId.hashCode() : 0);
        result = 31 * result + (dstVertexLabelId != null ? dstVertexLabelId.hashCode() : 0);
        return result;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(EdgeKind edgeKind) {
        return new Builder(edgeKind);
    }

    public static class Builder {

        private String edgeLabel;
        private LabelId edgeLabelId;
        private String srcVertexLabel;
        private LabelId srcVertexLabelId;
        private String dstVertexLabel;
        private LabelId dstVertexLabelId;

        private Builder() {}

        private Builder(EdgeKind edgeKind) {
            this.edgeLabel = edgeKind.getEdgeLabel();
            this.edgeLabelId = edgeKind.getEdgeLabelId();
            this.srcVertexLabel = edgeKind.getSrcVertexLabel();
            this.srcVertexLabelId = edgeKind.getSrcVertexLabelId();
            this.dstVertexLabel = edgeKind.getDstVertexLabel();
            this.dstVertexLabelId = edgeKind.getDstVertexLabelId();
        }

        public Builder setEdgeLabel(String edgeLabel) {
            this.edgeLabel = edgeLabel;
            return this;
        }

        public Builder setEdgeLabelId(LabelId edgeLabelId) {
            this.edgeLabelId = edgeLabelId;
            return this;
        }

        public Builder setSrcVertexLabel(String srcVertexLabel) {
            this.srcVertexLabel = srcVertexLabel;
            return this;
        }

        public Builder setSrcVertexLabelId(LabelId srcVertexLabelId) {
            this.srcVertexLabelId = srcVertexLabelId;
            return this;
        }

        public Builder setDstVertexLabel(String dstVertexLabel) {
            this.dstVertexLabel = dstVertexLabel;
            return this;
        }

        public Builder setDstVertexLabelId(LabelId dstVertexLabelId) {
            this.dstVertexLabelId = dstVertexLabelId;
            return this;
        }

        public EdgeKind build() {
            return new EdgeKind(
                    edgeLabel,
                    edgeLabelId,
                    srcVertexLabel,
                    srcVertexLabelId,
                    dstVertexLabel,
                    dstVertexLabelId);
        }
    }
}
