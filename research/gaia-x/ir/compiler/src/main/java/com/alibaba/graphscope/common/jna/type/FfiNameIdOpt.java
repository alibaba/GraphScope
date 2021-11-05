package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiNameIdOpt implements IntEnum<FfiNameIdOpt> {
    None,
    Name,
    Id;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiNameIdOpt getEnum(int i) {
        FfiNameIdOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
