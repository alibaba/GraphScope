package com.alibaba.maxgraph.v2.common.operation;

import com.alibaba.maxgraph.proto.v2.VertexIdPb;

public class VertexId {
    private long id;

    public VertexId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public VertexIdPb toProto() {
        return VertexIdPb.newBuilder().setId(id).build();
    }

    @Override
    public String toString() {
        return String.valueOf(id);
    }
}
