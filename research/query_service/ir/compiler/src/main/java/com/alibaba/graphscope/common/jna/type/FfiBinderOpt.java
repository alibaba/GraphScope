package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiBinderOpt implements IntEnum<FfiBinderOpt> {
    Edge,
    Path,
    Vertex;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiBinderOpt getEnum(int i) {
        FfiBinderOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
