package com.alibaba.maxgraph.v2.common.operation;

import com.alibaba.maxgraph.proto.v2.LabelIdPb;

public class LabelId {
    private int id;

    public LabelId(int id) {
        this.id = id;
    }

    public static LabelId parseProto(LabelIdPb proto) {
        int id = proto.getId();
        return new LabelId(id);
    }

    public int getId() {
        return id;
    }

    public LabelIdPb toProto() {
        return LabelIdPb.newBuilder().setId(id).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        LabelId labelId = (LabelId) o;

        return id == labelId.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return "LabelId{" +
                "id=" + id +
                '}';
    }
}
