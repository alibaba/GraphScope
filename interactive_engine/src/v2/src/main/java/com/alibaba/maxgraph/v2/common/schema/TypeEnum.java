package com.alibaba.maxgraph.v2.common.schema;

import com.alibaba.maxgraph.proto.v2.TypeEnumPb;

public enum TypeEnum {
    /**
     * vertex type
     */
    VERTEX(0),

    /**
     * edge type
     */
    EDGE(1);

    private final byte b;

    TypeEnum(int b) {
        this.b = (byte) b;
    }

    public static TypeEnum fromId(byte id) {
        if (id < 0 || id >= ALL.length) {
            throw new IllegalArgumentException("Unknown TypeEnum: [" + id + "]");
        }
        return ALL[id];
    }

    public static TypeEnum parseProto(TypeEnumPb proto) {
        return fromId((byte) proto.getNumber());
    }

    public TypeEnumPb toProto() {
        return TypeEnumPb.forNumber(b);
    }

    private static final TypeEnum[] ALL = TypeEnum.values();
}
