package com.alibaba.maxgraph.v2.common.operation;

import com.alibaba.maxgraph.proto.v2.EdgeIdPb;

public class EdgeId {

    private VertexId srcId;
    private VertexId dstId;
    private long id;

    public EdgeId(VertexId srcId, VertexId dstId, long id) {
        this.srcId = srcId;
        this.dstId = dstId;
        this.id = id;
    }

    public VertexId getSrcId() {
        return srcId;
    }

    public EdgeIdPb toProto() {
        return EdgeIdPb.newBuilder()
                .setSrcId(srcId.toProto())
                .setDstId(dstId.toProto())
                .setId(id)
                .build();
    }

    @Override
    public String toString() {
        return srcId + "-" + id + "->" + dstId;
    }
}
