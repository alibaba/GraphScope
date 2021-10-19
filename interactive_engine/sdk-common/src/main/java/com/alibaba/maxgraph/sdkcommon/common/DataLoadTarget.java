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
package com.alibaba.maxgraph.sdkcommon.common;

import com.alibaba.maxgraph.proto.DataLoadTargetPb;

public class DataLoadTarget {
    private String label;
    private String srcLabel;
    private String dstLabel;

    private int labelId;
    private int srcLabelId;
    private int dstLabelId;

    private DataLoadTarget(
            String label,
            String srcLabel,
            String dstLabel,
            int labelId,
            int srcLabelId,
            int dstLabelId) {
        this.label = label;
        this.srcLabel = srcLabel;
        this.dstLabel = dstLabel;

        this.labelId = labelId;
        this.srcLabelId = srcLabelId;
        this.dstLabelId = dstLabelId;
    }

    public String getLabel() {
        return label;
    }

    public String getSrcLabel() {
        return srcLabel;
    }

    public String getDstLabel() {
        return dstLabel;
    }

    public int getLabelId() {
        return labelId;
    }

    public int getSrcLabelId() {
        return srcLabelId;
    }

    public int getDstLabelId() {
        return dstLabelId;
    }

    public DataLoadTargetPb toProto() {
        DataLoadTargetPb.Builder builder = DataLoadTargetPb.newBuilder();
        builder.setLabel(this.label);
        builder.setLabelId(this.labelId);
        if (this.srcLabel != null) {
            builder.setSrcLabel(this.srcLabel);
            builder.setSrcLabelId(this.srcLabelId);
        }
        if (this.dstLabel != null) {
            builder.setDstLabel(this.dstLabel);
            builder.setDstLabelId(this.dstLabelId);
        }
        return builder.build();
    }

    public static DataLoadTarget parseProto(DataLoadTargetPb proto) {
        String label = proto.getLabel();
        String srcLabel = proto.getSrcLabel();
        String dstLabel = proto.getDstLabel();
        int labelId = proto.getLabelId();
        int srcLabelId = proto.getSrcLabelId();
        int dstLabelId = proto.getDstLabelId();
        return new DataLoadTarget(label, srcLabel, dstLabel, labelId, srcLabelId, dstLabelId);
    }

    @Override
    public String toString() {
        return "DataLoadTarget{"
                + "label='"
                + label
                + '\''
                + ", srcLabel='"
                + srcLabel
                + '\''
                + ", dstLabel='"
                + dstLabel
                + '\''
                + '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(DataLoadTarget target) {
        return new Builder(target);
    }

    public static class Builder {

        private String label;
        private int labelId;
        private String srcVertexLabel;
        private int srcVertexLabelId;
        private String dstVertexLabel;
        private int dstVertexLabelId;

        private Builder() {}

        private Builder(DataLoadTarget target) {
            this.label = target.getLabel();
            this.labelId = target.getLabelId();
            this.srcVertexLabel = target.getSrcLabel();
            this.srcVertexLabelId = target.getSrcLabelId();
            this.dstVertexLabel = target.getDstLabel();
            this.dstVertexLabelId = target.getDstLabelId();
        }

        public Builder setLabel(String edgeLabel) {
            this.label = edgeLabel;
            return this;
        }

        public Builder setLabelId(int edgeLabelId) {
            this.labelId = edgeLabelId;
            return this;
        }

        public Builder setSrcLabel(String srcVertexLabel) {
            this.srcVertexLabel = srcVertexLabel;
            return this;
        }

        public Builder setSrcLabelId(int srcVertexLabelId) {
            this.srcVertexLabelId = srcVertexLabelId;
            return this;
        }

        public Builder setDstLabel(String dstVertexLabel) {
            this.dstVertexLabel = dstVertexLabel;
            return this;
        }

        public Builder setDstLabelId(int dstVertexLabelId) {
            this.dstVertexLabelId = dstVertexLabelId;
            return this;
        }

        public DataLoadTarget build() {
            return new DataLoadTarget(
                    label,
                    srcVertexLabel,
                    dstVertexLabel,
                    labelId,
                    srcVertexLabelId,
                    dstVertexLabelId);
        }
    }
}
