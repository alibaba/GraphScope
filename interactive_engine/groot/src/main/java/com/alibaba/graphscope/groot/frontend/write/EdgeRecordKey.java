package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.proto.write.EdgeRecordKeyPb;

public class EdgeRecordKey {

    private String label;
    private VertexRecordKey srcVertexRecordKey;
    private VertexRecordKey dstVertexRecordKey;
    private long edgeInnerId;

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
