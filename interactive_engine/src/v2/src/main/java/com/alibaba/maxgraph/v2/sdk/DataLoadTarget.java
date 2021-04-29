package com.alibaba.maxgraph.v2.sdk;

import com.alibaba.maxgraph.proto.v2.DataLoadTargetPb;

public class DataLoadTarget {
    private String label;
    private String srcLabel;
    private String dstLabel;

    public DataLoadTarget(String label, String srcLabel, String dstLabel) {
        this.label = label;
        this.srcLabel = srcLabel;
        this.dstLabel = dstLabel;
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

    public DataLoadTargetPb toProto(){
        DataLoadTargetPb.Builder builder = DataLoadTargetPb.newBuilder();
        builder.setLabel(this.label);
        if (this.srcLabel != null) {
            builder.setSrcLabel(this.srcLabel);
        }
        if (this.dstLabel != null) {
            builder.setDstLabel(this.dstLabel);
        }
        return builder.build();
    }

    public static DataLoadTarget parseProto(DataLoadTargetPb proto) {
        String label = proto.getLabel();
        String srcLabel = proto.getSrcLabel();
        String dstLabel = proto.getDstLabel();
        return new DataLoadTarget(label, srcLabel, dstLabel);
    }

    @Override
    public String toString() {
        return "DataLoadTarget{" +
                "label='" + label + '\'' +
                ", srcLabel='" + srcLabel + '\'' +
                ", dstLabel='" + dstLabel + '\'' +
                '}';
    }
}
