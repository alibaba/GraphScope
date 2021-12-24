package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiAggOpt implements IntEnum<FfiAggOpt> {
    Sum,
    Min,
    Max,
    Count,
    CountDistinct,
    ToList,
    ToSet,
    Avg;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiAggOpt getEnum(int i) {
        FfiAggOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
