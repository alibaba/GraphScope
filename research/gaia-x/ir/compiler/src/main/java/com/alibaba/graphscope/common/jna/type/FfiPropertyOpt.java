package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiPropertyOpt implements IntEnum<FfiPropertyOpt> {
    None,
    Id,
    Label,
    Key;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiPropertyOpt getEnum(int i) {
        FfiPropertyOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
