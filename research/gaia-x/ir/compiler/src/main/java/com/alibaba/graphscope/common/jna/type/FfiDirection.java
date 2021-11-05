package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiDirection implements IntEnum<FfiDirection> {
    Out,
    In,
    Both;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiDirection getEnum(int i) {
        FfiDirection opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
