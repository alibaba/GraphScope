package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiResultOpt implements IntEnum<FfiResultOpt> {
    EndV,
    AllV;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiResultOpt getEnum(int i) {
        FfiResultOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
