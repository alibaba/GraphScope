package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiPathOpt implements IntEnum<FfiPathOpt> {
    Simple,
    VertexDuplicates;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiPathOpt getEnum(int i) {
        FfiPathOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
