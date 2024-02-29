package com.alibaba.graphscope.groot.common.util;

import com.alibaba.graphscope.proto.groot.EdgeRecordKeyPb;

public class EdgeRecordKey {

    private final String label;
    private final VertexRecordKey srcVertexRecordKey;
    private final VertexRecordKey dstVertexRecordKey;
    private final long edgeInnerId;

    public EdgeRecordKey(
            String label, VertexRecordKey srcVertexRecordKey, VertexRecordKey dstVertexRecordKey) {
        this(label, srcVertexRecordKey, dstVertexRecordKey, -1L);
    }

    public EdgeRecordKey(
            String label,
            VertexRecordKey srcVertexRecordKey,
            VertexRecordKey dstVertexRecordKey,
            long edgeInnerId) {
        this.label = label;
        this.srcVertexRecordKey = srcVertexRecordKey;
        this.dstVertexRecordKey = dstVertexRecordKey;
        this.edgeInnerId = edgeInnerId;
    }

    public String getLabel() {
        return label;
    }

    public VertexRecordKey getSrcVertexRecordKey() {
        return srcVertexRecordKey;
    }

    public VertexRecordKey getDstVertexRecordKey() {
        return dstVertexRecordKey;
    }

    public long getEdgeInnerId() {
        return edgeInnerId;
    }

    public static EdgeRecordKey parseProto(EdgeRecordKeyPb proto) {
        String label = proto.getLabel();
        long innerId = proto.getInnerId();
        VertexRecordKey srcVertexRecordKey = VertexRecordKey.parseProto(proto.getSrcVertexKey());
        VertexRecordKey dstVertexRecordKey = VertexRecordKey.parseProto(proto.getDstVertexKey());
        return new EdgeRecordKey(label, srcVertexRecordKey, dstVertexRecordKey, innerId);
    }

    public EdgeRecordKeyPb toProto() {
        return EdgeRecordKeyPb.newBuilder()
                .setLabel(label)
                .setSrcVertexKey(srcVertexRecordKey.toProto())
                .setDstVertexKey(dstVertexRecordKey.toProto())
                .build();
    }
}
