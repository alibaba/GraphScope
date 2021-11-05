package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiOrderOpt implements IntEnum<FfiOrderOpt> {
    Shuffle,
    Asc,
    Desc;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiOrderOpt getEnum(int i) {
        FfiOrderOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
