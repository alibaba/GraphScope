package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiDataType implements IntEnum<FfiDataType> {
    Unknown,
    Boolean,
    I32,
    I64,
    F64,
    Str;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiDataType getEnum(int i) {
        FfiDataType opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
