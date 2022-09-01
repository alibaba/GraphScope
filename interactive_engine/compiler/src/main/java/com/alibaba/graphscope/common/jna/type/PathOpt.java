package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum PathOpt implements IntEnum<PathOpt> {
    Arbitrary,
    Simple;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public PathOpt getEnum(int i) {
        PathOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
