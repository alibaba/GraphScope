package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiExpandOpt implements IntEnum<FfiExpandOpt> {
    Vertex,
    Edge,
    Degree;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiExpandOpt getEnum(int i) {
        FfiExpandOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
